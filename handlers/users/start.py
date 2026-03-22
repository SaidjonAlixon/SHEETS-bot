from aiogram import types, F
from aiogram.filters import CommandStart
from loader import dp

LOAD_BUTTONS = ("DELO", "MNK", "BUTATA", "AKA FS", "NYBC LLC")

@dp.message(CommandStart())
async def bot_start(message: types.Message):
    from keyboards.default.main_menu import load_select_menu
    await message.answer(f"Assalomu alaykum, {message.from_user.full_name}!\n"
                         f"Load Bot tizimiga xush kelibsiz. Load tanlang:", reply_markup=load_select_menu)


@dp.message(F.text.in_(LOAD_BUTTONS))
async def on_load_selected(message: types.Message):
    from keyboards.default.main_menu import get_main_menu
    from utils.company_storage import set_company
    load_name = message.text.strip()
    set_company(message.from_user.id, load_name)
    await message.answer(f"✅ {load_name} tanlandi. Iltimos bo'limni tanlang:", reply_markup=get_main_menu(message.from_user.id))
