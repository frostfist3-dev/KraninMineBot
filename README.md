# Telegram Script Notifier — starter

This is a starter service for testing a Telegram bot plus HTTPS API on Render.

## Files
- `app.py` — Telegram webhook and API endpoints.
- `requirements.txt` — Python dependencies.

## Render settings
Create a **Web Service** connected to this GitHub repository:
- Runtime: Python
- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- Health check path: `/`

Set these environment variables in Render:
- `BOT_TOKEN` — token from BotFather (secret; do not share it)
- `BOT_USERNAME` — bot username without `@`
- `BRIDGE_SECRET` — a long random secret, used by the Lua/server bridge
- `WEBHOOK_SECRET` — a different long random secret
- `PUBLIC_URL` — service URL, e.g. `https://your-service.onrender.com`

After first deploy, check the Render logs and open the service URL. It should return JSON with `"ok": true`.

## Important limitations of this starter
This version keeps pending link codes and subscribers in RAM. They are lost when the service restarts or redeploys. Use a persistent database such as PostgreSQL before real multi-user use.
It does not yet connect to a Lua script automatically. The script must request a one-time link code from `/api/link` and send events to `/api/event`, with `X-Bridge-Secret` header. Never put `BOT_TOKEN` in the Lua script.
The `/api/link` endpoint expects a random one-time code to be created by a trusted bridge/server; do not expose a shared bridge secret inside a public Lua script. For production, use a per-user signed pairing flow and persistent database.
