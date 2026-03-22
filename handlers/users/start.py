from aiogram import types, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from loader import dp

LOAD_BUTTONS = ("DELO", "MNK", "BUTATA", "AKA FS", "NYBC LLC")  # Admin: hammasi, oddiy user: faqat BUTATA


@dp.message(F.text == "❌ Bekor qilish")
async def cancel_action(message: types.Message, state: FSMContext):
    """Har qanday jarayonni bekor qilish va asosiy menyuga qaytish."""
    await state.clear()
    from keyboards.default.main_menu import get_main_menu
    await message.answer("Amal bekor qilindi. Asosiy menyu:", reply_markup=get_main_menu(message.from_user.id))


@dp.message(CommandStart())
async def bot_start(message: types.Message):
    from keyboards.default.main_menu import get_load_select_menu
    await message.answer(f"Assalomu alaykum, {message.from_user.full_name}!\n"
                         f"Load Bot tizimiga xush kelibsiz. Load tanlang:", reply_markup=get_load_select_menu(message.from_user.id))


@dp.message(F.text.in_(LOAD_BUTTONS))
async def on_load_selected(message: types.Message):
    from keyboards.default.main_menu import get_main_menu, get_load_select_menu
    from utils.access_control import is_admin
    from utils.company_storage import set_company
    load_name = message.text.strip()
    # Oddiy foydalanuvchi faqat BUTATA tanlashi mumkin
    if not is_admin(message.from_user.id) and load_name != "BUTATA":
        await message.answer("Iltimos, Load tanlang:", reply_markup=get_load_select_menu(message.from_user.id))
        return
    set_company(message.from_user.id, load_name)
    await message.answer(f"✅ {load_name} tanlandi. Iltimos bo'limni tanlang:", reply_markup=get_main_menu(message.from_user.id))
