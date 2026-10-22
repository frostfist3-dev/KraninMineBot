import asyncio
import json
import logging
import os
import re
import secrets
import time
from contextlib import asynccontextmanager
from typing import Dict, Set
from urllib.parse import parse_qs

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("kranin_bridge")


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Required environment variable {name} must be set and non-empty")
    return value


BOT_TOKEN = _required_env("BOT_TOKEN")
BOT_USERNAME = _required_env("BOT_USERNAME").lstrip("@")
PUBLIC_URL = _required_env("PUBLIC_URL").rstrip("/")
WEBHOOK_SECRET = _required_env("WEBHOOK_SECRET")
if not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", WEBHOOK_SECRET):
    raise RuntimeError("WEBHOOK_SECRET must contain 1-256 letters, numbers, '_' or '-'")
if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", BOT_USERNAME):
    raise RuntimeError("BOT_USERNAME must be the Telegram bot username without @")
if not PUBLIC_URL.startswith("https://") or any(c.isspace() for c in PUBLIC_URL):
    raise RuntimeError("PUBLIC_URL must be a valid HTTPS URL")

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "").strip()
GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"
DEFAULT_MODEL = "openai/gpt-oss-20b"
ALLOWED_MODELS = {"openai/gpt-oss-20b", "openai/gpt-oss-120b"}
AI_MIN_INTERVAL_SECONDS = max(1.0, float(os.environ.get("AI_MIN_INTERVAL_SECONDS", "2.5")))
AI_DAILY_IP_LIMIT = max(1, int(os.environ.get("AI_DAILY_IP_LIMIT", "300")))

# A single Uvicorn worker keeps the in-memory cache coherent; the JSON snapshot persists state.
# Cross-restart durability requires STATE_FILE to point at a persistent volume (for example /var/data).
code_to_chats: Dict[str, Set[int]] = {}
pending_codes: Dict[str, float] = {}
last_heartbeat: Dict[str, float] = {}
running_state: Dict[str, bool] = {}
crash_notified: Set[str] = set()
command_queues: Dict[str, list] = {}
command_receipts: Dict[str, dict] = {}
recent_commands: Dict[tuple, float] = {}
ai_last_request: Dict[str, float] = {}
ai_ip_daily: Dict[tuple, int] = {}
watchdog_task = None
webhook_task = None
tg_ready = False
webhook_registered = False
LINK_CODE_TTL = 60 * 60 * 24 * 30
HEARTBEAT_TIMEOUT = 90
QUEUE_TTL = 600
MAX_QUEUE_PER_INSTALL = 20
ALLOWED_COMMANDS = {"status", "stats", "stopbot", "startbot", "close_game"}
STATE_FILE = os.environ.get("STATE_FILE", "./kranin_state.json").strip() or "./kranin_state.json"


def _save_state() -> None:
    """Atomically persist pairing/command state. Durable across restarts when STATE_FILE is on a persistent disk."""
    payload = {
        "code_to_chats": {code: sorted(chats) for code, chats in code_to_chats.items()},
        "pending_codes": pending_codes,
        "last_heartbeat": last_heartbeat,
        "running_state": running_state,
        "crash_notified": sorted(crash_notified),
        "command_queues": command_queues,
        "command_receipts": command_receipts,
        "recent_commands": [[list(key), stamp] for key, stamp in recent_commands.items()],
        "ai_last_request": ai_last_request,
        "ai_ip_daily": [[ip, day, count] for (ip, day), count in ai_ip_daily.items()],
    }
    temp_path = STATE_FILE + ".tmp"
    try:
        parent = os.path.dirname(os.path.abspath(STATE_FILE))
        os.makedirs(parent, exist_ok=True)
        with open(temp_path, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, STATE_FILE)
    except Exception:
        logger.exception("Could not persist application state to %s", STATE_FILE)
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except OSError:
            pass


def _load_state() -> None:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as stream:
            payload = json.load(stream)
        for code, chats in payload.get("code_to_chats", {}).items():
            if re_safe_code(code) and isinstance(chats, list):
                code_to_chats[code] = {int(chat_id) for chat_id in chats}
        pending_codes.update({code: float(exp) for code, exp in payload.get("pending_codes", {}).items() if re_safe_code(code)})
        last_heartbeat.update({code: float(stamp) for code, stamp in payload.get("last_heartbeat", {}).items() if re_safe_code(code)})
        running_state.update({code: bool(flag) for code, flag in payload.get("running_state", {}).items() if re_safe_code(code)})
        crash_notified.update(code for code in payload.get("crash_notified", []) if re_safe_code(code))
        for code, queue in payload.get("command_queues", {}).items():
            if re_safe_code(code) and isinstance(queue, list):
                command_queues[code] = [item for item in queue if isinstance(item, dict) and item.get("command") in ALLOWED_COMMANDS]
        for receipt_id, receipt in payload.get("command_receipts", {}).items():
            if isinstance(receipt, dict) and re_safe_code(receipt.get("code", "")):
                command_receipts[str(receipt_id)] = receipt
        for item in payload.get("recent_commands", []):
            if isinstance(item, list) and len(item) == 2 and isinstance(item[0], list):
                recent_commands[tuple(item[0])] = float(item[1])
        ai_last_request.update({str(key): float(value) for key, value in payload.get("ai_last_request", {}).items()})
        for item in payload.get("ai_ip_daily", []):
            if isinstance(item, list) and len(item) == 3:
                ai_ip_daily[(str(item[0]), int(item[1]))] = int(item[2])
        logger.info("Loaded application state from %s", STATE_FILE)
    except FileNotFoundError:
        logger.info("No saved state yet at %s", STATE_FILE)
    except Exception:
        logger.exception("Could not load application state from %s; starting empty", STATE_FILE)


def re_safe_code(code: str) -> bool:
    return isinstance(code, str) and 16 <= len(code) <= 128 and all(
        ch in "0123456789abcdefABCDEF" for ch in code
    )


def _fix_cp1251(value: str) -> str:
    """Recover common legacy CP1251-as-Latin-1 mojibake without touching proper UTF-8 text."""
    if not value or any(ord(ch) > 255 for ch in value):
        return value
    # Only consider strings that look like legacy Cyrillic bytes interpreted as Latin-1.
    if not re.search(r"[\u00c0-\u00ff]{3,}", value):
        return value
    try:
        raw = value.encode("latin-1")
    except UnicodeEncodeError:
        return value
    try:
        repaired = raw.decode("utf-8")
        if repaired:
            return repaired
    except UnicodeDecodeError:
        pass
    try:
        repaired = raw.decode("cp1251")
        if repaired:
            return repaired
    except UnicodeDecodeError:
        pass
    return value


def _clean_state(now: float | None = None) -> None:
    now = time.time() if now is None else now
    changed = False
    for receipt_id, receipt in list(command_receipts.items()):
        if receipt.get("expires_at", 0) < now:
            command_receipts.pop(receipt_id, None)
            changed = True
    for code, queue in list(command_queues.items()):
        retained = []
        for item in queue:
            if now - item.get("created_at", now) > QUEUE_TTL:
                command_receipts.pop(item.get("id", ""), None)
                changed = True
            else:
                retained.append(item)
        if retained:
            if len(retained) != len(queue):
                command_queues[code] = retained
        else:
            command_queues.pop(code, None)
            if queue:
                changed = True
    for key, stamp in list(recent_commands.items()):
        if now - stamp > 10:
            recent_commands.pop(key, None)
            changed = True
    # A pairing code expires after 30 days without being re-linked.
    for code, expires_at in list(pending_codes.items()):
        if expires_at < now:
            pending_codes.pop(code, None)
            code_to_chats.pop(code, None)
            command_queues.pop(code, None)
            last_heartbeat.pop(code, None)
            running_state.pop(code, None)
            crash_notified.discard(code)
            for receipt_id, receipt in list(command_receipts.items()):
                if receipt.get("code") == code:
                    command_receipts.pop(receipt_id, None)
            changed = True
    current_day = int(now // 86400)
    for key in list(ai_ip_daily):
        if key[1] < current_day - 1:
            ai_ip_daily.pop(key, None)
            changed = True
    for client_id, stamp in list(ai_last_request.items()):
        if now - stamp > 86400:
            ai_last_request.pop(client_id, None)
            changed = True
    if changed:
        _save_state()


def _client_ip(request: Request) -> str:
    # Render supplies X-Forwarded-For. Fall back to the ASGI peer when not behind a proxy.
    forwarded = request.headers.get("x-forwarded-for", "").split(",", 1)[0].strip()
    return forwarded or (request.client.host if request.client else "unknown")


async def _request_fields(request: Request) -> dict:
    """Accept legacy GET query params and the new POST form/JSON body."""
    if request.method == "GET":
        return dict(request.query_params)
    try:
        body = await request.body()
    except Exception:
        return {}
    if not body:
        return {}
    content_type = request.headers.get("content-type", "").lower()
    if "application/json" in content_type:
        try:
            payload = await request.json()
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}
    parsed = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
    return {key: values[-1] if values else "" for key, values in parsed.items()}


def _field_str(fields: dict, key: str, default: str = "") -> str:
    value = fields.get(key, default)
    if value is None:
        return default
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    return default


async def queue_command_for_chat(chat_id: int, command: str) -> dict:
    if command not in ALLOWED_COMMANDS:
        raise ValueError("Unsupported command")
    now = time.time()
    _clean_state(now)
    linked_codes = [code for code, chats in code_to_chats.items() if chat_id in chats]
    added = 0
    duplicate = 0
    offline = 0
    for code in linked_codes:
        last_seen = last_heartbeat.get(code, 0)
        if not last_seen or now - last_seen > 60:
            offline += 1
        dedupe_key = (code, chat_id, command)
        previous = recent_commands.get(dedupe_key, 0)
        if now - previous < 10:
            duplicate += 1
            continue
        queue = command_queues.setdefault(code, [])
        if len(queue) >= MAX_QUEUE_PER_INSTALL:
            continue
        command_id = secrets.token_hex(8)
        queue.append({
            "id": command_id,
            "command": command,
            "chat_id": chat_id,
            "created_at": now,
        })
        command_receipts[command_id] = {
            "code": code,
            "chat_id": chat_id,
            "created_at": now,
            "expires_at": now + QUEUE_TTL,
        }
        recent_commands[dedupe_key] = now
        added += 1
    if added:
        _save_state()
    return {
        "linked": len(linked_codes),
        "added": added,
        "duplicate": duplicate,
        "offline": offline,
    }


async def enqueue_from_chat(update: Update, command: str):
    if not update.effective_chat or not update.message:
        return
    result = await queue_command_for_chat(update.effective_chat.id, command)
    if not result["linked"]:
        await update.message.reply_text(
            "Этот чат не привязан к игре. Открой скрипт → подключение Telegram → Start."
        )
        return
    if result["added"] == 0 and result["duplicate"]:
        await update.message.reply_text("ℹ️ Такая команда уже отправлялась недавно; повтор не добавляю в очередь.")
        return
    if result["added"] == 0:
        await update.message.reply_text("⚠️ Очередь заполнена. Подожди немного и повтори команду.")
        return
    message = f"⌛ Команда добавлена в очередь для {result['added']} установки."
    if result["offline"]:
        message += (
            f"\n⚠️ Для {result['offline']} установки нет heartbeat более 60 секунд. "
            "Команда сработает, только когда скрипт снова свяжется с сервером, "
            "и истечёт через 10 минут."
        )
    else:
        message += "\nСкрипт должен быть запущен и иметь связь с сервером."
    await update.message.reply_text(message)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.message:
        return
    code = context.args[0] if context.args else ""
    if code and re_safe_code(code):
        chats = code_to_chats.setdefault(code, set())
        chats.add(update.effective_chat.id)
        pending_codes[code] = time.time() + LINK_CODE_TTL
        _save_state()
        await update.message.reply_text(
            "✅ Telegram подключён к этой установке. Уведомления будут приходить сюда.\n"
            "Команды: /status, /stats, /stopbot, /startbot. Закрытие игры требует подтверждения.\n"
            "/stop отключает уведомления и команды в этом чате."
        )
    else:
        await update.message.reply_text(
            "Привет! Открой Lua-скрипт → раздел подключения Telegram → нажми ссылку на бота, затем Start."
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
                command_queues.pop(code, None)
                for receipt_id, receipt in list(command_receipts.items()):
                    if receipt.get("code") == code:
                        command_receipts.pop(receipt_id, None)
    if removed:
        _save_state()
    await update.message.reply_text(
        "Уведомления и команды отключены в этом чате." if removed else "Этот чат не был привязан к установке."
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await enqueue_from_chat(update, "status")


async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await enqueue_from_chat(update, "stats")


async def stopbot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await enqueue_from_chat(update, "stopbot")


async def startbot_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await enqueue_from_chat(update, "startbot")


async def closegame_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.message or not update.effective_user:
        return
    user_id = update.effective_user.id
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Да, закрыть игру", callback_data=f"close_yes:{user_id}"),
        InlineKeyboardButton("Отмена", callback_data=f"close_no:{user_id}"),
    ]])
    await update.message.reply_text(
        "⚠️ Закрытие игры может привести к потере несохранённого прогресса. Отправить команду на закрытие?",
        reply_markup=keyboard,
    )


async def closegame_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.message or not query.from_user:
        return
    try:
        action, expected_user = (query.data or "").split(":", 1)
        if int(expected_user) != query.from_user.id:
            await query.answer("Эта кнопка предназначена тому, кто вызвал команду.", show_alert=True)
            return
    except (ValueError, TypeError):
        await query.answer("Кнопка недействительна.", show_alert=True)
        return
    if action == "close_no":
        await query.answer("Отменено")
        await query.edit_message_text("✅ Закрытие игры отменено.")
        return
    if action != "close_yes":
        await query.answer("Неизвестное действие.", show_alert=True)
        return
    await query.answer("Запрос отправляется")
    result = await queue_command_for_chat(query.message.chat_id, "close_game")
    if not result["linked"]:
        await query.edit_message_text("Этот чат не привязан к игре; команда не отправлена.")
        return
    if not result["added"]:
        await query.edit_message_text("Команда уже была отправлена недавно или очередь заполнена.")
        return
    message = "⌛ Подтверждённая команда на закрытие игры поставлена в очередь."
    if result["offline"]:
        message += "\n⚠️ Скрипт не отвечал более 60 секунд; запрос истечёт через 10 минут."
    await query.edit_message_text(message)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.message:
        return
    await update.message.reply_text(
        "Команды Kranin Mine:\n"
        "/status — состояние скрипта и лицензии\n"
        "/stats — статистика добычи и время работы\n"
        "/stopbot — остановить автоматизацию в игре\n"
        "/startbot — запустить автоматизацию, если лицензия активна\n"
        "/closegame — запросить закрытие игры (нужно подтвердить кнопкой)\n"
        "/stop — отключить уведомления и команды в этом чате"
    )


tg_app = Application.builder().token(BOT_TOKEN).build()
tg_app.add_handler(CommandHandler("start", start_cmd))
tg_app.add_handler(CommandHandler("stop", stop_cmd))
tg_app.add_handler(CommandHandler("help", help_cmd))
tg_app.add_handler(CommandHandler("status", status_cmd))
tg_app.add_handler(CommandHandler("stats", stats_cmd))
tg_app.add_handler(CommandHandler("stopbot", stopbot_cmd))
tg_app.add_handler(CommandHandler("startbot", startbot_cmd))
tg_app.add_handler(CommandHandler("closegame", closegame_cmd))
tg_app.add_handler(CallbackQueryHandler(closegame_callback, pattern=r"^close_(yes|no):\d+$"))


async def _webhook_registration_loop() -> None:
    global webhook_registered
    delay = 2
    while True:
        try:
            await tg_app.bot.set_webhook(
                url=f"{PUBLIC_URL}/telegram/webhook",
                secret_token=WEBHOOK_SECRET,
                allowed_updates=["message", "callback_query"],
            )
            webhook_registered = True
            logger.info("Telegram webhook registered")
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            webhook_registered = False
            logger.exception("Telegram webhook registration failed; retrying in %s seconds", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)


async def crash_watchdog() -> None:
    while True:
        await asyncio.sleep(15)
        now = time.time()
        _clean_state(now)
        for code, last_seen in list(last_heartbeat.items()):
            if running_state.get(code, False) and now - last_seen > HEARTBEAT_TIMEOUT:
                if code not in crash_notified:
                    crash_notified.add(code)
                    await notify_code(
                        code,
                        "🔴 ВОЗМОЖНЫЙ ВЫЛЕТ / ПОТЕРЯ СВЯЗИ\n"
                        "Скрипт перестал присылать сигналы. Это не подтверждает вылет.",
                    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global watchdog_task, webhook_task, tg_ready
    _load_state()
    _clean_state()
    await tg_app.initialize()
    await tg_app.start()
    tg_ready = True
    webhook_task = asyncio.create_task(_webhook_registration_loop())
    watchdog_task = asyncio.create_task(crash_watchdog())
    try:
        yield
    finally:
        tg_ready = False
        for task in (webhook_task, watchdog_task):
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        webhook_task = None
        watchdog_task = None
        _save_state()
        await tg_app.stop()
        await tg_app.shutdown()


app = FastAPI(title="Kranin Mine Telegram + AI Bridge", lifespan=lifespan)


async def notify_code(code: str, message: str):
    for chat_id in list(code_to_chats.get(code, set())):
        try:
            await tg_app.bot.send_message(chat_id=chat_id, text=message)
        except Exception:
            logger.exception("Could not send Telegram notification for code %s", code[:6])


@app.get("/")
async def root():
    return {
        "ok": True,
        "service": "kranin-mine-bridge",
        "telegram_initialized": tg_ready,
        "webhook_registered": webhook_registered,
        "ai_proxy_configured": bool(GROQ_API_KEY),
    }


@app.get("/health")
async def health():
    if not tg_ready or not webhook_registered:
        raise HTTPException(
            status_code=503,
            detail={"telegram_initialized": tg_ready, "webhook_registered": webhook_registered},
        )
    return {"ok": True, "telegram_initialized": True, "webhook_registered": True}


@app.get("/telegram/link")
async def telegram_link(code: str):
    if not re_safe_code(code):
        raise HTTPException(status_code=400, detail="Invalid pairing code")
    return RedirectResponse(url=f"https://t.me/{BOT_USERNAME}?start={code}", status_code=302)


@app.post("/telegram/webhook")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str = Header(default=""),
):
    if not secrets.compare_digest(x_telegram_bot_api_secret_token, WEBHOOK_SECRET):
        raise HTTPException(status_code=403, detail="Forbidden")
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    update = Update.de_json(payload, tg_app.bot)
    await tg_app.process_update(update)
    return {"ok": True}


@app.api_route("/telegram/event", methods=["GET", "POST"])
async def telegram_event(request: Request):
    fields = await _request_fields(request)
    code = _field_str(fields, "code")
    event_key = _field_str(fields, "event").strip().lower()
    running = _field_str(fields, "running", "0")
    running_now = running.lower() in {"1", "true", "yes"}
    info = _fix_cp1251(_field_str(fields, "info"))[:500].strip()
    if not re_safe_code(code):
        raise HTTPException(status_code=400, detail="Invalid pairing code")

    if event_key == "heartbeat":
        now = time.time()
        was_lost = code in crash_notified
        last_heartbeat[code] = now
        running_state[code] = running_now
        if was_lost and running_now:
            crash_notified.discard(code)
            await notify_code(code, "🟢 СВЯЗЬ ВОССТАНОВЛЕНА — скрипт снова отвечает.")
        _save_state()
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
        "admin_attention": "⚠️ ВОЗМОЖНОЕ ОБРАЩЕНИЕ / ПРОВЕРКА В ЧАТЕ",
    }
    if event_key not in labels:
        raise HTTPException(status_code=400, detail="Unsupported event")
    # Do not send a second stop message immediately after a target_reached event.
    if event_key == "stop" and re.search(r"goal\s*reached|цель\s+достигнута", info, re.IGNORECASE):
        running_state[code] = False
        _save_state()
        return {"ok": True, "event": event_key, "suppressed": True}

    running_state[code] = running_now
    if event_key == "start":
        last_heartbeat[code] = time.time()
        if code in crash_notified:
            crash_notified.discard(code)
            await notify_code(code, "🟢 СВЯЗЬ ВОССТАНОВЛЕНА — бот снова запущен.")
    if event_key in ("stop", "script_unloaded"):
        crash_notified.discard(code)

    message = labels[event_key]
    if info:
        message += "\n" + info
    await notify_code(code, message)
    _save_state()
    return {"ok": True, "event": event_key, "linked_chats": len(code_to_chats.get(code, set()))}


@app.api_route("/telegram/commands", methods=["GET", "POST"])
async def telegram_commands(request: Request):
    fields = await _request_fields(request)
    code = _field_str(fields, "code")
    if not re_safe_code(code):
        raise HTTPException(status_code=400, detail="Invalid pairing code")
    if not code_to_chats.get(code):
        raise HTTPException(status_code=403, detail="Installation is not linked to a Telegram chat")
    now = time.time()
    _clean_state(now)
    queue = command_queues.setdefault(code, [])
    while queue:
        item = queue.pop(0)
        if now - item.get("created_at", now) > QUEUE_TTL:
            command_receipts.pop(item.get("id", ""), None)
            continue
        if item.get("command") not in ALLOWED_COMMANDS:
            command_receipts.pop(item.get("id", ""), None)
            continue
        _save_state()
        return {"ok": True, "id": item["id"], "command": item["command"]}
    command_queues.pop(code, None)
    return {"ok": True, "id": "", "command": None}


@app.api_route("/telegram/command-result", methods=["GET", "POST"])
async def telegram_command_result(request: Request):
    fields = await _request_fields(request)
    code = _field_str(fields, "code")
    receipt_id = _field_str(fields, "id")
    result = _field_str(fields, "result", "Command handled")
    if not re_safe_code(code):
        raise HTTPException(status_code=400, detail="Invalid pairing code")
    # Validate first; only consume the receipt after the Telegram reply succeeds.
    receipt = command_receipts.get(receipt_id)
    if not receipt or receipt.get("code") != code or receipt.get("expires_at", 0) < time.time():
        raise HTTPException(status_code=403, detail="Invalid or expired command receipt")
    chat_id = receipt.get("chat_id")
    if chat_id not in code_to_chats.get(code, set()):
        raise HTTPException(status_code=403, detail="Telegram chat is no longer linked")
    safe_result = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", _fix_cp1251(result))[:240]
    try:
        await tg_app.bot.send_message(chat_id=chat_id, text="✅ Результат команды: " + (safe_result or "готово"))
    except Exception:
        logger.exception("Could not deliver a command result")
        raise HTTPException(status_code=502, detail="Could not send command result to Telegram")
    command_receipts.pop(receipt_id, None)
    _save_state()
    return {"ok": True}


@app.post("/api/ai/chat")
async def ai_chat(request: Request, x_client_id: str = Header(default="")):
    """OpenAI-compatible proxy. Groq key remains server-side; IDs must belong to linked installs."""
    if not GROQ_API_KEY:
        raise HTTPException(status_code=503, detail="Server AI key is not configured")
    client_id = x_client_id.strip()
    if not re_safe_code(client_id):
        raise HTTPException(status_code=401, detail="Invalid client installation ID")
    if not code_to_chats.get(client_id):
        raise HTTPException(status_code=403, detail="Installation is not linked to a Telegram chat")

    now = time.time()
    _clean_state(now)
    last_at = ai_last_request.get(client_id, 0)
    if now - last_at < AI_MIN_INTERVAL_SECONDS:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit: wait {max(1, int(AI_MIN_INTERVAL_SECONDS - (now - last_at)) + 1)} seconds",
            headers={"Retry-After": str(max(1, int(AI_MIN_INTERVAL_SECONDS - (now - last_at)) + 1))},
        )
    ip = _client_ip(request)
    day_key = (ip, int(now // 86400))
    if ai_ip_daily.get(day_key, 0) >= AI_DAILY_IP_LIMIT:
        raise HTTPException(status_code=429, detail="Daily AI request limit reached for this IP")

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

    requested_model = body.get("model", DEFAULT_MODEL)
    model = requested_model if isinstance(requested_model, str) and requested_model in ALLOWED_MODELS else DEFAULT_MODEL
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
    # Count only requests that pass validation and are actually sent upstream.
    ai_last_request[client_id] = now
    ai_ip_daily[day_key] = ai_ip_daily.get(day_key, 0) + 1
    _save_state()
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
        logger.exception("Groq proxy request failed")
        raise HTTPException(status_code=502, detail="Could not reach Groq API")
