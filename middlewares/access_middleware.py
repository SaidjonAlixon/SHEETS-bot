"""
Dostup tekshiruvi: faqat ruxsat berilgan buxgalterlar botdan foydalana oladi.
Kutish yo'q – barcha dostupi borlar bir vaqtda ishlashi mumkin.
"""
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery
from typing import Callable, Dict, Any, Awaitable
import config
from utils.access_control import has_access, is_admin


class AccessMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message | CallbackQuery,
        data: Dict[str, Any]
    ) -> Any:
        msg = event.message if isinstance(event, CallbackQuery) else event
        user_id = event.from_user.id if event.from_user else 0
        text = ""
        if isinstance(event, Message) and event.text:
            text = (event.text or "").strip().lower().split()[0]

        # /myid – hamma ishlata oladi (ID olish uchun)
        if text == "/myid":
            return await handler(event, data)

        # Admin har doim o'tadi
        if is_admin(user_id):
            return await handler(event, data)

        # Dostup yo'q
        if not has_access(user_id):
            try:
                await msg.answer(
                    "⛔ Siz bu Telegram botni ishlata olmaysiz.\n\n"
                    "Dostup admin tomonidan beriladi. /myid yuborib ID oling va admin bilan bog'laning."
                )
            except Exception:
                pass
            return

        return await handler(event, data)
