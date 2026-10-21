# Kranin Mine Telegram Bridge (compatible with current Lua GET requests)

## Render environment variables
- `BOT_TOKEN`: BotFather token (server-side only)
- `BOT_USERNAME`: bot username without `@`
- `WEBHOOK_SECRET`: long random secret
- `PUBLIC_URL`: `https://kraninminebot.onrender.com`

## Render commands
Build: `pip install -r requirements.txt`
Start: `uvicorn app:app --host 0.0.0.0 --port $PORT`

## Lua config
In `moonloader/config/mbot.ini`:
- `telegram.enabled = true` is set when the user presses the connect button
- `telegram.bot_username = YOUR_BOT_USERNAME` must be changed to the actual bot username
- `telegram.bridge_url = https://kraninminebot.onrender.com/telegram/event`

Then restart the script, open the Telegram connection button, and press Start in the bot.

## Limitations / security
This is a starter implementation. Pairing codes are per-install bearer secrets, so users must keep `config/mbot.ini` private and never share a pairing code. The current server stores bindings in RAM and loses them on restart/redeploy; add PostgreSQL before relying on it. Heartbeat loss is only a possible crash/network loss, not proof of a crash. This implementation does not send admin-threat alerts.
