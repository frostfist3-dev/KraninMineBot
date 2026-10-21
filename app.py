import asyncio
import os
import re
import secrets
import time
from typing import Dict, Set

from fastapi import FastAPI, Header, HTTPException, Request
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_TOKEN = os.environ["BOT_TOKEN"]
BOT_USERNAME = os.environ["BOT_USERNAME"].lstrip("@")
PUBLIC_URL = os.environ["PUBLIC_URL"].rstrip("/")
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]

app = FastAPI(title="Kranin Mine Telegram Bridge")
tg_app = Application.builder().token(BOT_TOKEN).build()

# Starter storage only. Restart/redeploy clears these maps; use PostgreSQL
# before relying on notifications long-term.
code_to_chats: Dict[str, Set[int]] = {}
pending_codes: Dict[str, float] = {}
last_heartbeat: Dict[str, float] = {}
running_state: Dict[str, bool] = {}
crash_notified: Set[str] = set()

LINK_CODE_TTL = 60 * 60 * 24 * 30
HEARTBEAT_TIMEOUT = 90


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.message:
        return
    code = context.args[0] if context.args else ""
    if code and re_safe_code(code):
        # The install code is a bearer secret. Only people who possess it can
        # bind their Telegram chat to that installation.
        chats = code_to_chats.setdefault(code, set())
        chats.add(update.effective_chat.id)
        pending_codes[code] = time.time() + LINK_CODE_TTL
        await update.message.reply_text(
            "✅ Telegram подключён к этой установке. Уведомления будут приходить сюда.\n"
            "Команда /stop отключает уведомления."
        )
    else:
        await update.message.reply_text(
            "Привет! Открой Lua-скрипт → раздел «О авторе» → «Подключить уведомления Telegram», "
            "затем вернись сюда и нажми Start."
        )


def re_safe_code(code: str) -> bool:
    return 16 <= len(code) <= 128 and all(ch in "0123456789abcdefABCDEF" for ch in code)


async def stop_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.message:
        return
    chat_id = update.effective_chat.id
    removed = False
    for code, chats in list(code_to_chats.items()):
        if chat_id in chats:
            chats.discard(chat_id)
            removed = True
            if not chats:
                code_to_chats.pop(code, None)
    await update.message.reply_text(
        "Уведомления отключены." if removed else "Этот чат не был привязан к установке."
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.message:
        return
    chat_id = update.effective_chat.id
    linked = any(chat_id in chats for chats in code_to_chats.values())
    await update.message.reply_text(
        "✅ Чат привязан. Ожидаю события скрипта." if linked else
        "Чат пока не привязан. Нажми кнопку подключения в Lua-скрипте и затем Start."
    )


tg_app.add_handler(CommandHandler("start", start_cmd))
tg_app.add_handler(CommandHandler("stop", stop_cmd))
tg_app.add_handler(CommandHandler("status", status_cmd))


@app.on_event("startup")
async def startup():
    await tg_app.initialize()
    await tg_app.start()
    await tg_app.bot.set_webhook(
        url=f"{PUBLIC_URL}/telegram/webhook",
        secret_token=WEBHOOK_SECRET,
        allowed_updates=["message"],
    )
    asyncio.create_task(crash_watchdog())


@app.on_event("shutdown")
async def shutdown():
    await tg_app.bot.delete_webhook()
    await tg_app.stop()
    await tg_app.shutdown()


async def crash_watchdog():
    # Best-effort detector: heartbeat loss can mean a crash, network loss,
    # game exit, or Render sleeping. It cannot prove the game crashed.
    while True:
        await asyncio.sleep(15)
        now = time.time()
        for code, last_seen in list(last_heartbeat.items()):
            if running_state.get(code, False) and now - last_seen > HEARTBEAT_TIMEOUT:
                if code not in crash_notified:
                    crash_notified.add(code)
                    await notify_code(
                        code,
                        "🔴 ВОЗМОЖНЫЙ ВЫЛЕТ / ПОТЕРЯ СВЯЗИ\n"
                        "Скрипт перестал присылать сигналы. Это не подтверждает вылет."
                    )


async def notify_code(code: str, text: str):
    for chat_id in list(code_to_chats.get(code, set())):
        try:
            await tg_app.bot.send_message(chat_id=chat_id, text=text)
        except Exception:
            # Continue delivering to other linked chats if one has blocked the bot.
            pass


@app.get("/")
async def root():
    return {"ok": True, "service": "kranin-mine-telegram-bridge"}


@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str = Header(default="")
):
    if not secrets.compare_digest(x_telegram_bot_api_secret_token, WEBHOOK_SECRET):
        raise HTTPException(status_code=403, detail="Forbidden")
    payload = await request.json()
    update = Update.de_json(payload, tg_app.bot)
    await tg_app.process_update(update)
    return {"ok": True}


@app.get("/telegram/event")
async def telegram_event(
    code: str,
    event: str,
    running: str = "0",
    info: str = ""
):
    if not re_safe_code(code):
        raise HTTPException(status_code=400, detail="Invalid pairing code")

    # Any caller must know the installation's high-entropy pairing code.
    # Never reuse a code across installations; keep config/mbot.ini private.
    event_key = event.strip().lower()
    running_now = running == "1"
    info = info[:500].strip()

    if event_key == "heartbeat":
        last_heartbeat[code] = time.time()
        running_state[code] = running_now
        if running_now:
            crash_notified.discard(code)
        return {"ok": True, "event": "heartbeat"}

    labels = {
        "start": "🟢 СТАРТ",
        "stop": "⏹️ СТОП",
        "script_unloaded": "⏹️ СКРИПТ ОСТАНОВЛЕН / ВЫГРУЖЕН",
        "license": "🔑 ПРОБЛЕМА С ЛИЦЕНЗИЕЙ",
        "license_error": "🔑 ПРОБЛЕМА С ЛИЦЕНЗИЕЙ",
        "status": "📊 СТАТУС",
        "crash": "🔴 ВОЗМОЖНЫЙ ВЫЛЕТ / ПОТЕРЯ СВЯЗИ",
    }
    if event_key not in labels:
        raise HTTPException(status_code=400, detail="Unsupported event")

    running_state[code] = running_now
    if event_key == "start":
        last_heartbeat[code] = time.time()
        crash_notified.discard(code)
    if event_key in ("stop", "script_unloaded"):
        crash_notified.discard(code)

    text = labels[event_key]
    if info:
        text += "\n" + info
    await notify_code(code, text)
    return {"ok": True, "event": event_key, "linked_chats": len(code_to_chats.get(code, set()))}
