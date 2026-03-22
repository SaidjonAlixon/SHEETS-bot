from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

broker_menu = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="📤 To'lov faylini yuklash"),
            KeyboardButton(text="📋 Oxirgi to'lovlar"),
        ],
        [
            KeyboardButton(text="⬅️ Orqaga"),
            KeyboardButton(text="❌ Bekor qilish"),
        ],
    ],
    resize_keyboard=True
)

expenses_menu = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="⬅️ Back (Main Menu)"),
            KeyboardButton(text="❌ Bekor qilish"),
        ],
    ],
    resize_keyboard=True
)
