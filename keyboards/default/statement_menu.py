from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

statement_menu = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="🚚 Owner Operator"),
            KeyboardButton(text="👷 Contractor"),
        ],
        [
            KeyboardButton(text="🏢 Company Driver"),
        ],
        [
            KeyboardButton(text="📤 Fayl yuklash"),
            KeyboardButton(text="📈 Solishtirish natijalari"),
        ],
        [
            KeyboardButton(text="⬅️ Back (Main Menu)"),
            KeyboardButton(text="❌ Bekor qilish"),
        ],
    ],
    resize_keyboard=True
)
