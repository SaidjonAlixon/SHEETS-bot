import asyncio
import logging
import sys
from loader import dp, bot
from middlewares import AccessMiddleware
from middlewares.activity_log_middleware import ActivityLogMiddleware
import handlers

# Dostup middleware
dp.message.middleware(AccessMiddleware())
dp.callback_query.middleware(AccessMiddleware())
# Aktivlik loglari (DB ga yozish)
dp.message.middleware(ActivityLogMiddleware())
dp.callback_query.middleware(ActivityLogMiddleware())

# Loggingni sozlash
logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def _migrate_json_to_db():
    """Eski JSON dan allowed_users ni DB ga ko'chirish."""
    import json
    from pathlib import Path
    from database.db import get_db
    from utils.access_control import grant_access
    path = Path(__file__).parent / "data" / "access_control.json"
    if not path.exists():
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        allowed = data.get("allowed", {})
        if isinstance(allowed, list):
            allowed = {str(u): {} for u in allowed}
        for uid, info in allowed.items():
            try:
                uid_int = int(uid)
                grant_access(uid_int, info.get("username", ""), info.get("full_name", ""))
            except Exception:
                pass
        print("JSON -> DB migratsiya bajarildi.")
    except Exception as e:
        print(f"Migratsiya xato: {e}")

async def main():
    try:
        _migrate_json_to_db()
        print("Bot ishga tushmoqda...")
        # Start command
        await dp.start_polling(bot)
    except Exception as e:
        print(f"Xatolik yuz berdi: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot to'xtatildi!")
