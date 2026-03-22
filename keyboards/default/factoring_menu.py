from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

factoring_menu = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="📤 Fayl yuklash (Excel xlsx/xls)"),
            KeyboardButton(text="📋 Oxirgi yuklamalar"),
        ],
        [
            KeyboardButton(text="🔍 Yuk raqami bo'yicha qidirish"),
            KeyboardButton(text="📊 Hisobot olish"),
        ],
        [
            KeyboardButton(text="⬅️ Orqaga")
        ]
    ],
    resize_keyboard=True
)
