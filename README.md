# KraninMine Telegram + Groq AI server

## Render environment
Keep the existing variables:
- `BOT_TOKEN` — the token for your Telegram bot
- `BOT_USERNAME` — your actual bot username without `@`; the server uses it to build the Telegram start link
- `WEBHOOK_SECRET` — the current webhook secret
- `PUBLIC_URL` — `https://kraninminebot.onrender.com`

Add one secret environment variable in Render:
- `GROQ_API_KEY` — the Groq API key owned by the script publisher

The Groq key stays on the server and is never embedded in the Lua file or shown to script users.

## Deploy
Replace the current service's `app.py` and `requirements.txt` with these files, then deploy. Start command:
`uvicorn app:app --host 0.0.0.0 --port $PORT`

The root endpoint reports `ai_proxy_configured: true` when the Groq environment variable is present. `/api/ai/chat` is an OpenAI-compatible `POST` endpoint. The bridge applies in-memory per-installation/per-IP request limits; Telegram links and rate counters reset after a server restart. For production, store notification bindings and usage quotas in a database.

## Lua file
Use `V2_Telegram_Render_READY.lua`. The script no longer requires `bot_username` in `mbot.ini`: the Telegram button opens `/telegram/link`, and the server redirects to the bot using `BOT_USERNAME` already configured on Render. The AI endpoint defaults to `https://kraninminebot.onrender.com/api/ai/chat`. Existing default direct-Groq settings migrate to the proxy; custom non-Groq endpoints are preserved.
