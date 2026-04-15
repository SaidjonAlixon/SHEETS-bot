from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from utils.access_control import is_admin
from utils.access_control import is_super_admin

def get_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    """Statement Check — faqat super admin; Sozlamalar adminlar uchun."""
    base = [
        [
            KeyboardButton(text="📄 Factoring Payments"),
            KeyboardButton(text="💰 Broker Payments"),
        ],
        [
            KeyboardButton(text="⛽ Fuel Expenses"),
            KeyboardButton(text="🛣️ Toll Expenses"),
        ],
    ]
    if is_super_admin(user_id):
        base.append([KeyboardButton(text="📊 Statement Check")])
    if is_admin(user_id):
        base.append([KeyboardButton(text="⚙️ Sozlamalar")])
    return ReplyKeyboardMarkup(keyboard=base, resize_keyboard=True)

def get_load_select_menu(user_id: int) -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text="DELO"), KeyboardButton(text="MNK")],
        [KeyboardButton(text="BUTATA"), KeyboardButton(text="AKA FS")],
        [KeyboardButton(text="NYBC LLC")],
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# Oddiy menyu (get_main_menu ishlatiladi)
main_menu = None  # get_main_menu(user_id) orqali oling

back_button = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="⬅️ Back")]
    ],
    resize_keyboard=True
)
