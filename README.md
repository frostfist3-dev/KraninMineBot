# KraninMine Telegram + Groq AI server

## Render environment
Keep these existing variables:
- `BOT_TOKEN` — Telegram bot token
- `BOT_USERNAME` — bot username without `@`
- `WEBHOOK_SECRET` — Telegram webhook secret
- `PUBLIC_URL` — `https://kraninminebot.onrender.com`
- `GROQ_API_KEY` — Groq API key stored only in Render environment variables

## Deploy
Replace `app.py` and `requirements.txt` in the existing Render service, then deploy.
Start command:
`uvicorn app:app --host 0.0.0.0 --port $PORT`

The root endpoint reports `ai_proxy_configured: true` when `GROQ_API_KEY` is configured. `/api/ai/chat` accepts OpenAI-compatible chat-completion requests. The app no longer applies its own per-installation or per-IP request-count quotas. Groq's own account, model, concurrency and rate limits still apply, and timeouts/input validation remain enabled.

**Security note:** with application-level quotas removed, anyone who discovers the public proxy URL and can construct a valid hexadecimal client ID may attempt to use the shared Groq key. Monitor Groq usage and rotate the key if abuse is suspected. Do not put `GROQ_API_KEY` into Lua files.

## Lua file
Use `KRANINmineV4_ADMIN_ALERT_NO_AI_CAP.lua`. The default AI endpoint is `https://kraninminebot.onrender.com/api/ai/chat`; the shared Groq key remains on Render.

## Administrator message detection
Lines beginning with the explicit `Администратор`, `Administrator`, `Admin:` and supported `[A]`/`[Адм]` prefixes bypass Groq's yes/no classifier. This keeps the previous heuristic active for system notices like transport spawning, forum applications, and TSP release/jail messages. Explicit administrator-prefixed lines trigger immediately; ambiguous text-only matches are throttled to one Telegram alert every 45 seconds. The original protection/auto-reply branch keeps its own existing safety checks. This is text-pattern recognition, not cryptographic verification of a player's role.
