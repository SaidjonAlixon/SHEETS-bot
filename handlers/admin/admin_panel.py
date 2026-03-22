import os
import tempfile
from aiogram import types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from loader import dp, bot
import config
import pandas as pd
from states.bot_states import BotStates
from utils.access_control import (
    grant_access,
    revoke_access,
    get_allowed_count,
    get_allowed_list,
    set_global_enabled,
    is_global_enabled,
    is_admin,
    grant_admin,
    revoke_admin,
    get_admin_list,
)
from database.db import get_db

def admin_only(func):
    async def wrapper(event, *args, **kwargs):
        user_id = event.from_user.id if event.from_user else 0
        if not is_admin(user_id):
            return
        return await func(event, *args, **kwargs)
    return wrapper

@dp.message(Command("admin"))
@admin_only
async def admin_panel_cmd(message: types.Message, state: FSMContext, **kwargs):
    await _admin_panel(message, state)

@dp.message(F.text == "⚙️ Sozlamalar")
async def settings_btn(message: types.Message, state: FSMContext):
    if is_admin(message.from_user.id):
        await _admin_panel(message, state)

def admin_menu_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Dostup berish", callback_data="admin:grant")],
        [InlineKeyboardButton(text="➖ Dostup olib tashlash", callback_data="admin:revoke")],
        [InlineKeyboardButton(text="👑 Admin qo'shish", callback_data="admin:add_admin")],
        [InlineKeyboardButton(text="👑 Admin olib tashlash", callback_data="admin:revoke_admin")],
        [InlineKeyboardButton(text="👑 Adminlar ro'yxati", callback_data="admin:admin_list")],
        [InlineKeyboardButton(text="📋 Ro'yxat", callback_data="admin:list")],
        [InlineKeyboardButton(text="📜 Aktivlik loglari", callback_data="admin:logs")],
        [InlineKeyboardButton(
            text=("🔴 Dostupni o'chirish" if is_global_enabled() else "🟢 Dostupni qo'yish"),
            callback_data="admin:toggle"
        )],
    ])

@dp.message(Command("myid"))
async def cmd_myid(message: types.Message):
    """Hamma /myid yuborishi mumkin - o'z ID sini olish uchun."""
    uid = message.from_user.id if message.from_user else 0
    name = (message.from_user.full_name or "User") if message.from_user else ""
    await message.answer(
        f"🆔 Sizning Telegram ID: <code>{uid}</code>\n\n"
        f"Buni adminga yuboring dostup olish uchun."
    )

async def _admin_panel(message: types.Message, state: FSMContext):
    await state.clear()
    count = get_allowed_count()
    enabled = is_global_enabled()
    status = "Qo'yilgan ✅" if enabled else "O'chirilgan ❌"
    await message.answer(
        f"⚙️ <b>Admin panel</b>\n\n"
        f"📊 Dostup berilganlar: <b>{count}</b> kishi\n"
        f"🔐 Global dostup: {status}\n\n"
        f"Quyidagi tugmalardan birini tanlang:",
        reply_markup=admin_menu_keyboard()
    )

@dp.callback_query(F.data == "admin:logs_back")
async def logs_back_callback(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await callback.answer()
    await state.clear()
    await callback.message.edit_text(
        "⚙️ <b>Admin panel</b>\n\nQuyidagi tugmalardan birini tanlang:",
        reply_markup=admin_menu_keyboard()
    )


@dp.callback_query(F.data.startswith("logs_user:"))
async def logs_user_callback(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await callback.answer()
    try:
        user_id = int(callback.data.replace("logs_user:", "").strip())
    except ValueError:
        return
    db = get_db()
    if not db or not db.connection:
        await callback.message.edit_text("❌ Bazaga ulanish xatosi.", reply_markup=admin_menu_keyboard())
        return
    logs = db.get_logs_by_user(user_id)
    if not logs:
        await callback.message.edit_text(
            "📜 Ushbu foydalanuvchi uchun aktivlik topilmadi.",
            reply_markup=admin_menu_keyboard()
        )
        return
    user_name = (logs[0].get("full_name") or logs[0].get("username") or f"ID:{user_id}").strip()

    def _action_label(a):
        a = (a or "").strip()
        al = a.lower()
        if al == "callback": return "Tugma bosildi"
        if al == "message": return "Xabar/menyu"
        if al == "document": return "Fayl yuklandi"
        return a or "-"

    rows = []
    for L in logs:
        ts = L.get("timestamp") or ""
        if hasattr(ts, "strftime"):
            ts = ts.strftime("%Y-%m-%d %H:%M:%S")
        else:
            ts = str(ts)[:19]
        action = _action_label(L.get("action"))
        details = (L.get("details") or "")[:500]
        result = (L.get("result") or "")
        rows.append({"Sana va vaqt": ts, "Harakat turi": action, "Tafsilot": details, "Natija": result})
    df = pd.DataFrame(rows)
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    path = tmp.name
    tmp.close()
    try:
        df.to_excel(path, index=False, sheet_name="Aktivliklar")
        await callback.message.answer_document(
            types.FSInputFile(path),
            caption=f"📜 <b>{user_name}</b> (ID: {user_id}) — dostup berilgandan beri barcha aktivliklar"
        )
        await callback.message.edit_text(
            f"✅ Excel fayl yuborildi: {user_name}",
            reply_markup=admin_menu_keyboard()
        )
    except Exception as e:
        await callback.message.edit_text(f"❌ Excel yaratishda xatolik: {e}", reply_markup=admin_menu_keyboard())
    finally:
        if os.path.exists(path):
            os.remove(path)


@dp.callback_query(F.data.startswith("admin:"))
async def admin_callback(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Ruxsat yo'q.", show_alert=True)
        return
    await callback.answer()

    action = callback.data.replace("admin:", "")
    if action == "grant":
        await state.set_state(BotStates.AdminAddAccess)
        await callback.message.edit_text(
            "➕ <b>Dostup berish</b>\n\n"
            "User ID ni kiriting (faqat raqam, masalan: 123456789):\n\n"
            "Bekor qilish: /admin"
        )
    elif action == "revoke":
        await state.set_state(BotStates.AdminRevokeAccess)
        await callback.message.edit_text(
            "➖ <b>Dostup olib tashlash</b>\n\n"
            "User ID ni kiriting:\n\nBekor qilish: /admin"
        )
    elif action == "add_admin":
        await state.set_state(BotStates.AdminAddAdmin)
        await callback.message.edit_text(
            "👑 <b>Admin qo'shish</b>\n\n"
            "User ID ni kiriting (faqat raqam, masalan: 123456789):\n\n"
            "Bekor qilish: /admin"
        )
    elif action == "revoke_admin":
        await state.set_state(BotStates.AdminRevokeAdmin)
        await callback.message.edit_text(
            "👑 <b>Admin olib tashlash</b>\n\n"
            "User ID ni kiriting (faqat .env dan qo'shilgan adminlarni o'chirib bo'lmaydi):\n\n"
            "Bekor qilish: /admin"
        )
    elif action == "admin_list":
        await state.clear()
        admins = get_admin_list()
        lines = ["👑 <b>Adminlar:</b>\n"]
        for i, (uid, username, full_name) in enumerate(admins[:30], 1):
            name = full_name or username or f"ID:{uid}"
            lines.append(f"{i}. {name} (<code>{uid}</code>)")
        if len(admins) > 30:
            lines.append(f"\n... va yana {len(admins)-30} ta")
        await callback.message.edit_text("\n".join(lines), reply_markup=admin_menu_keyboard())
    elif action == "list":
        await state.clear()
        count = get_allowed_count()
        users = get_allowed_list()
        lines = [f"📋 <b>Dostup berilganlar: {count} kishi</b>\n"]
        for i, (uid, username, full_name) in enumerate(users[:30], 1):
            name = full_name or username or f"ID:{uid}"
            lines.append(f"{i}. {name} (<code>{uid}</code>)")
        if len(users) > 30:
            lines.append(f"\n... va yana {len(users)-30} ta")
        await callback.message.edit_text("\n".join(lines), reply_markup=admin_menu_keyboard())
    elif action == "logs":
        await state.clear()
        db = get_db()
        users = db.get_users_with_activity() if db and db.connection else []
        if not users:
            await callback.message.edit_text(
                "📜 Aktivlik loglari bo'sh. Hali hech kim botdan foydalanmagan.",
                reply_markup=admin_menu_keyboard()
            )
            return
        buttons = []
        row = []
        for u in users[:30]:
            uid = u.get("user_id")
            name = (u.get("full_name") or u.get("username") or f"ID:{uid}")[:25]
            row.append(InlineKeyboardButton(text=name, callback_data=f"logs_user:{uid}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([InlineKeyboardButton(text="⬅️ Orqaga", callback_data="admin:logs_back")])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await callback.message.edit_text(
            "📜 <b>Aktivlik loglari</b>\n\n"
            "Qaysi foydalanuvchining aktivliklarini Excel ko'rinishida olishni xohlaysiz?",
            reply_markup=kb
        )
    elif action == "toggle":
        new_val = not is_global_enabled()
        set_global_enabled(new_val)
        status = "qo'yildi ✅" if new_val else "o'chirildi ❌"
        await callback.message.edit_text(
            f"Global dostup {status}.\n\n"
            f"Endi {'barcha ruxsatlilar' if new_val else 'faqat adminlar'} botdan foydalana oladi.",
            reply_markup=admin_menu_keyboard()
        )

@dp.message(BotStates.AdminAddAccess, F.text.in_(["/admin", "❌ Bekor qilish"]))
@admin_only
async def admin_add_cancel(message: types.Message, state: FSMContext, **kwargs):
    await state.clear()
    await _admin_panel(message, state)

@dp.message(BotStates.AdminAddAccess, F.text)
@admin_only
async def admin_add_access(message: types.Message, state: FSMContext, **kwargs):
    text = message.text.strip()
    if not text.isdigit():
        await message.answer("❌ Faqat raqam kiriting (User ID).")
        return
    user_id = int(text)
    try:
        chat = await bot.get_chat(user_id)
        username = getattr(chat, "username", "") or ""
        full_name = getattr(chat, "full_name", "") or ""
    except Exception:
        username = ""
        full_name = ""

    grant_access(user_id, username, full_name)
    await state.clear()
    await message.answer(
        f"✅ Dostup berildi: ID {user_id}\n"
        f"Ism: {full_name or '-'}",
        reply_markup=admin_menu_keyboard()
    )

@dp.message(BotStates.AdminRevokeAccess, F.text.in_(["/admin", "❌ Bekor qilish"]))
@admin_only
async def admin_revoke_cancel(message: types.Message, state: FSMContext, **kwargs):
    await state.clear()
    await _admin_panel(message, state)

@dp.message(BotStates.AdminRevokeAccess, F.text)
@admin_only
async def admin_revoke_access(message: types.Message, state: FSMContext, **kwargs):
    text = message.text.strip()
    if not text.isdigit():
        await message.answer("❌ Faqat raqam kiriting (User ID).")
        return
    user_id = int(text)
    if revoke_access(user_id):
        await state.clear()
        await message.answer(f"✅ Dostup olib tashlandi: ID {user_id}", reply_markup=admin_menu_keyboard())
    else:
        await message.answer(f"⚠️ ID {user_id} ro'yxatda yo'q edi.")


@dp.message(BotStates.AdminAddAdmin, F.text.in_(["/admin"]))
@admin_only
async def admin_add_admin_cancel(message: types.Message, state: FSMContext, **kwargs):
    await state.clear()
    await _admin_panel(message, state)


@dp.message(BotStates.AdminAddAdmin, F.text)
@admin_only
async def admin_add_admin(message: types.Message, state: FSMContext, **kwargs):
    text = message.text.strip()
    if not text.isdigit():
        await message.answer("❌ Faqat raqam kiriting (User ID).")
        return
    user_id = int(text)
    try:
        chat = await bot.get_chat(user_id)
        username = getattr(chat, "username", "") or ""
        full_name = getattr(chat, "full_name", "") or ""
    except Exception:
        username = ""
        full_name = ""

    if grant_admin(user_id, username, full_name):
        await state.clear()
        await message.answer(
            f"✅ Admin qo'shildi: ID {user_id}\n"
            f"Ism: {full_name or '-'}",
            reply_markup=admin_menu_keyboard()
        )
    else:
        await message.answer("❌ Xatolik. Qayta urinib ko'ring.")


@dp.message(BotStates.AdminRevokeAdmin, F.text.in_(["/admin"]))
@admin_only
async def admin_revoke_admin_cancel(message: types.Message, state: FSMContext, **kwargs):
    await state.clear()
    await _admin_panel(message, state)


@dp.message(BotStates.AdminRevokeAdmin, F.text)
@admin_only
async def admin_revoke_admin(message: types.Message, state: FSMContext, **kwargs):
    text = message.text.strip()
    if not text.isdigit():
        await message.answer("❌ Faqat raqam kiriting (User ID).")
        return
    user_id = int(text)
    if str(user_id) in [str(a).strip() for a in (config.ADMINS or []) if a]:
        await message.answer(
            "⚠️ .env faylidagi adminni olib tashlab bo'lmaydi.\n"
            "Faqat bot orqali qo'shilgan adminlarni olib tashlash mumkin."
        )
        return
    if revoke_admin(user_id):
        await state.clear()
        await message.answer(f"✅ Admin olib tashlandi: ID {user_id}", reply_markup=admin_menu_keyboard())
    else:
        await message.answer(f"⚠️ ID {user_id} adminlar ro'yxatida yo'q edi.")
