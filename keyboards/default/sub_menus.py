from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

broker_menu = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="📤 To'lov faylini yuklash"),
            KeyboardButton(text="📋 Oxirgi to'lovlar"),
        ],
        [
            KeyboardButton(text="⬅️ Orqaga")
        ]
    ],
    resize_keyboard=True
)

expenses_menu = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="⬅️ Back (Main Menu)")
        ]
    ],
    resize_keyboard=True
)
