"""Foydalanuvchi harakatlarini bazaqa yozish."""
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery
from typing import Callable, Dict, Any, Awaitable
from database.db import get_db

class ActivityLogMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable,
        event: Message | CallbackQuery,
        data: Dict[str, Any]
    ) -> Any:
        result = await handler(event, data)
        try:
            user = event.from_user
            if not user:
                return result
            user_id = user.id
            username = getattr(user, "username", "") or ""
            full_name = getattr(user, "full_name", "") or ""
            action = ""
            details = ""
            if isinstance(event, Message):
                text = (event.text or "").strip()[:200]
                if event.document:
                    action = "document"
                    details = event.document.file_name or ""
                elif text:
                    cmd = text.split()[0] if text else ""
                    if cmd.startswith("/"):
                        action = cmd
                    else:
                        action = "message"
                        details = text[:100]
            elif isinstance(event, CallbackQuery):
                action = "callback"
                details = (event.data or "")[:100]
            if action:
                db = get_db()
                if db and db.connection:
                    db.add_log(user_id, action, details, username=username, full_name=full_name)
        except Exception:
            pass
        return result
