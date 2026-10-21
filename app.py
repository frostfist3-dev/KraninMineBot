import asyncio
import os
import re
import secrets
import time
from collections import defaultdict, deque
from typing import Deque, Dict, Set

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

BOT_TOKEN = os.environ["BOT_TOKEN"]
BOT_USERNAME = os.environ["BOT_USERNAME"].lstrip("@")
PUBLIC_URL = os.environ["PUBLIC_URL"].rstrip("/")
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"]
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "").strip()
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"
DEFAULT_MODEL = "openai/gpt-oss-20b"

app = FastAPI(title="Kranin Mine Telegram + AI Bridge")
tg_app = Application.builder().token(BOT_TOKEN).build()

# Starter storage: deploy/restart clears notification links and in-memory rate counters.
code_to_chats: Dict[str, Set[int]] = {}
pending_codes: Dict[str, float] = {}
last_heartbeat: Dict[str, float] = {}
running_state: Dict[str, bool] = {}
crash_notified: Set[str] = set()
ai_by_client: Dict[str, Deque[float]] = defaultdict(deque)
ai_by_ip: Dict[str, Deque[float]] = defaultdict(deque)

LINK_CODE_TTL = 60 * 60 * 24 * 30
HEARTBEAT_TIMEOUT = 90
AI_CLIENT_LIMIT = 15       # requests per 60 seconds per installation
AI_IP_LIMIT = 120          # requests per 60 minutes per IP


def re_safe_code(code: str) -> bool:
    return 16 <= len(code) <= 128 and all(ch in "0123456789abcdefABCDEF" for ch in code)


def within_limit(bucket: Dict[str, Deque[float]], key: str, limit: int, period: int) -> bool:
    now = time.time()
    hits = bucket[key]
    while hits and now - hits[0] >= period:
        hits.popleft()
    if len(hits) >= limit:
        return False
    hits.append(now)
    return True


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.message:
        return
    code = context.args[0] if context.args else ""
    if code and re_safe_code(code):
        chats = code_to_chats.setdefault(code, set())
        chats.add(update.effective_chat.id)
        pending_codes[code] = time.time() + LINK_CODE_TTL
        await update.message.reply_text(
            "✅ Telegram подключён к этой установке. Уведомления будут приходить сюда.\n"
            "Команда /stop отключает уведомления."
        )
    else:
        await update.message.reply_text(
            "Привет! Открой Lua-скрипт → раздел «Об авторе» → «Подключить уведомления Telegram», "
            "затем вернись сюда и нажми Start."
        )


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
    # Heartbeat loss can mean a crash, network loss, game exit, or Render sleeping.
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
            pass


@app.get("/")
async def root():
    return {
        "ok": True,
        "service": "kranin-mine-bridge",
        "ai_proxy_configured": bool(GROQ_API_KEY),
    }


@app.get("/telegram/link")
async def telegram_link(code: str):
    if not re_safe_code(code):
        raise HTTPException(status_code=400, detail="Invalid pairing code")
    return RedirectResponse(url=f"https://t.me/{BOT_USERNAME}?start={code}", status_code=302)


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
async def telegram_event(code: str, event: str, running: str = "0", info: str = ""):
    if not re_safe_code(code):
        raise HTTPException(status_code=400, detail="Invalid pairing code")
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
        "target_reached": "✅ ЦЕЛЬ ДОСТИГНУТА",
    }
    if event_key not in labels:
        raise HTTPException(status_code=400, detail="Unsupported event")

    running_state[code] = running_now
    if event_key == "start":
        last_heartbeat[code] = time.time()
        crash_notified.discard(code)
    if event_key in ("stop", "script_unloaded"):
        crash_notified.discard(code)

    message = labels[event_key]
    if info:
        message += "\n" + info
    await notify_code(code, message)
    return {"ok": True, "event": event_key, "linked_chats": len(code_to_chats.get(code, set()))}


@app.post("/api/ai/chat")
async def ai_chat(request: Request, x_client_id: str = Header(default="")):
    """OpenAI-compatible proxy. Groq key stays on the server, not in client Lua."""
    if not GROQ_API_KEY:
        raise HTTPException(status_code=503, detail="Server AI key is not configured")
    client_id = x_client_id.strip()
    if not re_safe_code(client_id):
        raise HTTPException(status_code=401, detail="Invalid client installation ID")

    ip = request.client.host if request.client else "unknown"
    if not within_limit(ai_by_client, client_id, AI_CLIENT_LIMIT, 60):
        raise HTTPException(status_code=429, detail="Client AI rate limit exceeded")
    if not within_limit(ai_by_ip, ip, AI_IP_LIMIT, 3600):
        raise HTTPException(status_code=429, detail="Network AI rate limit exceeded")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="JSON object required")
    messages = body.get("messages")
    if not isinstance(messages, list) or not messages or len(messages) > 20:
        raise HTTPException(status_code=400, detail="messages must contain 1-20 items")
    clean_messages = []
    total_chars = 0
    for item in messages:
        if not isinstance(item, dict) or item.get("role") not in ("system", "user", "assistant"):
            raise HTTPException(status_code=400, detail="Invalid message")
        content = item.get("content")
        if not isinstance(content, str):
            raise HTTPException(status_code=400, detail="Message content must be text")
        total_chars += len(content)
        if total_chars > 12000:
            raise HTTPException(status_code=413, detail="Messages too large")
        clean_messages.append({"role": item["role"], "content": content})

    model = body.get("model", DEFAULT_MODEL)
    if not isinstance(model, str) or not model or len(model) > 100:
        model = DEFAULT_MODEL
    try:
        max_tokens = int(body.get("max_tokens", 60))
    except (TypeError, ValueError):
        max_tokens = 60
    max_tokens = max(1, min(max_tokens, 120))
    try:
        temperature = float(body.get("temperature", 0.7))
    except (TypeError, ValueError):
        temperature = 0.7
    temperature = max(0.0, min(temperature, 1.5))

    payload = {
        "model": model,
        "messages": clean_messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                GROQ_ENDPOINT,
                headers={
                    "Authorization": f"Bearer {GROQ_API_KEY}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                json=payload,
            )
        try:
            content = response.json()
        except Exception:
            content = {"error": {"message": response.text[:1000]}}
        return JSONResponse(status_code=response.status_code, content=content)
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Groq request timed out")
    except httpx.HTTPError:
        raise HTTPException(status_code=502, detail="Could not reach Groq API")
