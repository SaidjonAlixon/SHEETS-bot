from aiogram import types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from loader import dp, bot
from keyboards.default.sub_menus import expenses_menu
from keyboards.default.main_menu import get_main_menu, load_select_menu
from states.bot_states import BotStates
from utils.company_storage import get_company

@dp.message(F.text == "⛽ Fuel Expenses")
async def enter_fuel(message: types.Message, state: FSMContext):
    company = get_company(message.from_user.id)
    if not company:
        await message.answer("Iltimos, avval Load tanlang:", reply_markup=load_select_menu)
        return
    await state.set_state(BotStates.Fuel)
    await message.answer("Iltimos, Excel (xlsx, xls) fayl yuboring.", reply_markup=expenses_menu)

@dp.message(F.text == "🛣️ Toll Expenses")
async def enter_toll(message: types.Message, state: FSMContext):
    company = get_company(message.from_user.id)
    if not company:
        await message.answer("Iltimos, avval Load tanlang:", reply_markup=load_select_menu)
        return
    await state.set_state(BotStates.Toll)
    await message.answer("Toll Expenses bo'limi.\n"
                         "Iltimos, Excel (xlsx, xls) faylini yuboring.", reply_markup=expenses_menu)

@dp.message(F.text == "⬅️ Back (Main Menu)", BotStates.Fuel)
@dp.message(F.text == "⬅️ Back (Main Menu)", BotStates.Toll)
async def back_main(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Asosiy menyu:", reply_markup=get_main_menu(message.from_user.id))

@dp.message(F.document, BotStates.Fuel)
async def handle_fuel_doc(message: types.Message, state: FSMContext):
    await handle_expense_doc(message, "FUEL", state)

@dp.message(F.document, BotStates.Toll)
async def handle_toll_doc(message: types.Message, state: FSMContext):
    await handle_expense_doc(message, "TOLL", state)

async def handle_expense_doc(message: types.Message, expense_type: str, state: FSMContext):
    company = get_company(message.from_user.id)
    if not company:
        await message.answer("Iltimos, avval Load tanlang:", reply_markup=load_select_menu)
        return
    document = message.document
    file_id = document.file_id
    file_name = document.file_name

    if expense_type == "FUEL" and not ("xlsx" in file_name.lower() or "xls" in file_name.lower()):
        await message.answer("⚠️ Fuel uchun faqat Excel (xlsx, xls) fayl yuboring.")
        return

    if expense_type == "TOLL" and not ("xlsx" in file_name.lower() or "xls" in file_name.lower()):
        await message.answer("⚠️ Toll uchun faqat Excel (xlsx, xls) fayl yuboring.")
        return

    await message.answer(f"{expense_type} fayl qabul qilindi. Qayta ishlanmoqda... ⏳")

    try:
        file = await bot.get_file(file_id)
        file_content = await bot.download_file(file.file_path)
        content_bytes = file_content.read()

        from services.google_sheets import get_sheet_service
        sheet_service = get_sheet_service()

        import pandas as pd
        import io
        import re

        # -------------------- FUEL --------------------
        if expense_type == "FUEL":
            # A = Card (sheets C4 dan boshlab)
            # B = Trans Date (shu sanaga mos list tanlash uchun)
            # S = Fuel summasi -> Fuel after discount
            # Q = Disc Amt -> Discount ustuniga (Toll Exp emas)
            # 1 fayl ichida bir xil card bir necha bor chiqsa: tanlangan list doirasida hammasini jamlaymiz.

            try:
                # Card id (A ustun) uzun bo'lgani uchun uni string sifatida o'qish kerak
                df = pd.read_excel(io.BytesIO(content_bytes), dtype=str)
            except Exception:
                await message.answer("❌ Fuel uchun xlsx faylni o‘qib bo‘lmadi.")
                return

            # Indekslar (0-based): A=0, B=1, Q=16, S=18
            if df.shape[1] <= 18:
                await message.answer("❌ Fuel xlsx faylda kamida 19 ta ustun bo‘lishi kerak (Q va S indekslar uchun).")
                return

            def parse_money(val):
                if val is None or (isinstance(val, float) and val != val):
                    return 0.0
                if pd.isna(val):
                    return 0.0
                s = str(val).strip().replace("$", "").replace(" ", "")
                # Decimal vergul bo'lishi mumkin: "2,13" => "2.13"
                if "," in s and "." in s:
                    s = s.replace(",", "")
                elif "," in s and "." not in s:
                    s = s.replace(",", ".")
                m = re.search(r'-?\d+(\.\d+)?', s)
                if not m:
                    return 0.0
                try:
                    return float(m.group())
                except ValueError:
                    return 0.0

            # Parse: card + date -> fuel/discount summa
            # S = Fuel Amount, Q = Disc Amt -> Discount ustuniga yoziladi (Toll Exp emas)
            entries_acc = {}  # (card, date_iso) -> [fuel_sum, discount_sum]
            progress_step = max(1, int(len(df) / 20))
            processed = 0
            last_progress = await message.answer("⏳ Fuel xlsx o‘qilmoqda... 0%")

            def normalize_card_str(v: str) -> str:
                if v is None:
                    return ""
                s = str(v).strip()
                if not s or s.lower() == "nan":
                    return ""
                # ilmiy yozuv bo'lsa
                if "e" in s.lower():
                    try:
                        from decimal import Decimal
                        return str(int(Decimal(s)))
                    except Exception:
                        pass
                # trailing .0
                if s.endswith(".0"):
                    return s[:-2]
                return s

            for _, row in df.iterrows():
                card_val = normalize_card_str(row.iloc[0])
                date_val = row.iloc[1]
                q_val = row.iloc[16]  # Q = Disc Amt
                s_val = row.iloc[18]  # S = Fuel Amount

                if not card_val:
                    continue
                try:
                    trans_date = pd.to_datetime(date_val).date()
                except Exception:
                    continue

                card_str = card_val
                date_iso = trans_date.isoformat()

                discount_sum = parse_money(q_val)  # Disc Amt -> Discount ustuniga
                fuel_sum = parse_money(s_val)

                key = (card_str, date_iso)
                if key not in entries_acc:
                    entries_acc[key] = [0.0, 0.0]
                entries_acc[key][0] += fuel_sum
                entries_acc[key][1] += discount_sum

                processed += 1
                if processed % progress_step == 0:
                    pct = int((processed / max(1, len(df))) * 100)
                    try:
                        await last_progress.edit_text(f"⏳ Fuel xlsx ishlanmoqda... {pct}%")
                    except Exception:
                        pass

            try:
                await last_progress.edit_text("✅ Fayl tayyor. Qaysi listni tekshiramiz?")
            except Exception:
                pass

            # Sheets listlarini ko‘rsatamiz (expenses spreadsheetdan)
            expenses_sheet_names = sheet_service.get_expenses_all_sheet_names(company)
            range_re = re.compile(r'(\d{1,2}\.\d{1,2})\s*-\s*(\d{1,2}\.\d{1,2})')
            sheet_candidates = [s for s in expenses_sheet_names if range_re.search(s)]
            if not sheet_candidates:
                sheet_candidates = expenses_sheet_names

            # Statega saqlaymiz (discount = Disc Amt, Discount ustuniga yoziladi)
            fuel_entries = []
            for (card, date_iso), (fuel_sum, discount_sum) in entries_acc.items():
                fuel_entries.append({"card": card, "date": date_iso, "fuel": fuel_sum, "discount": discount_sum})

            await state.set_state(BotStates.FuelSheetSelect)
            await state.update_data(
                fuel_entries=fuel_entries,
                fuel_sheet_names=sheet_candidates,
                fuel_filename=file_name,
                selected_company=company,
            )

            # Inline keyboard (2 ustun)
            buttons = []
            row = []
            for i, name in enumerate(sheet_candidates):
                row.append(InlineKeyboardButton(text=name, callback_data=f"fuel_sheet:{i}"))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            kb = InlineKeyboardMarkup(inline_keyboard=buttons)

            await message.answer(
                "📋 Qaysi listni tekshiramiz?\n"
                "Tugmani bosing — natija tez orada chiqadi.",
                reply_markup=kb
            )
            return

        # -------------------- TOLL (PrePass Customer Toll Details) --------------------
        if expense_type == "TOLL":
            # Sheet1: A=Post Date, E=PP Device ID, W=Toll $
            # PP Device ID ni sheetsdagi Transponder (D) bn solishtirib, hafta bo'yicha Toll Exp ga yoziladi
            try:
                # PrePass: header row 9 (0-indexed: 8), data row 10+
                df = pd.read_excel(io.BytesIO(content_bytes), sheet_name=0, header=8, dtype=str)
            except Exception:
                try:
                    df = pd.read_excel(io.BytesIO(content_bytes), sheet_name=0, dtype=str)
                except Exception:
                    await message.answer("❌ Toll uchun xlsx faylni o'qib bo'lmadi.")
                    return

            if df.shape[1] <= 22:
                await message.answer("❌ Toll xlsx faylda kamida 23 ta ustun bo'lishi kerak (W indeksi uchun).")
                return

            def parse_money(val):
                if val is None or (isinstance(val, float) and val != val) or pd.isna(val):
                    return 0.0
                s = str(val).strip().replace("$", "").replace(" ", "").replace(",", "")
                m = re.search(r'-?\d+(\.\d+)?', s)
                if not m:
                    return 0.0
                try:
                    return float(m.group())
                except ValueError:
                    return 0.0

            def normalize_transponder(v):
                if v is None or pd.isna(v):
                    return ""
                s = str(v).strip()
                if not s or s.lower() == "nan":
                    return ""
                if s.endswith(".0"):
                    s = s[:-2]
                return s

            # (pp_device_id, date_iso) -> toll_sum (jamlash)
            entries_acc = {}
            for _, row in df.iterrows():
                date_val = row.iloc[0]   # A = Post Date
                pp_id = normalize_transponder(row.iloc[4])   # E = PP Device ID
                toll_val = parse_money(row.iloc[22])         # W = Toll $

                if not pp_id:
                    continue
                try:
                    trans_date = pd.to_datetime(date_val).date()
                except Exception:
                    continue

                key = (pp_id, trans_date.isoformat())
                if key not in entries_acc:
                    entries_acc[key] = 0.0
                entries_acc[key] += toll_val

            toll_entries = [
                {"transponder": pp_id, "date": date_iso, "toll": toll_sum}
                for (pp_id, date_iso), toll_sum in entries_acc.items()
            ]

            if not toll_entries:
                await message.answer("❌ Toll faylda ma'lumot topilmadi (A, E, W ustunlari tekshirilsin).")
                return

            expenses_sheet_names = sheet_service.get_expenses_all_sheet_names(company)
            range_re = re.compile(r'(\d{1,2}\.\d{1,2})\s*-\s*(\d{1,2}\.\d{1,2})')
            sheet_candidates = [s for s in expenses_sheet_names if range_re.search(s)]
            if not sheet_candidates:
                sheet_candidates = expenses_sheet_names

            await state.set_state(BotStates.TollSheetSelect)
            await state.update_data(
                toll_entries=toll_entries,
                toll_sheet_names=sheet_candidates,
                toll_filename=file_name,
                selected_company=company,
            )

            buttons = []
            row_btns = []
            for i, name in enumerate(sheet_candidates):
                row_btns.append(InlineKeyboardButton(text=name, callback_data=f"toll_sheet:{i}"))
                if len(row_btns) == 2:
                    buttons.append(row_btns)
                    row_btns = []
            if row_btns:
                buttons.append(row_btns)
            kb = InlineKeyboardMarkup(inline_keyboard=buttons)

            await message.answer(
                "✅ Fayl tayyor. Qaysi listni tekshiramiz?\n"
                "Tugmani bosing — natija tez orada chiqadi.",
                reply_markup=kb
            )
            return

        await message.answer("❌ Noma'lum expense_type.")

    except Exception as e:
        await message.answer(f"❌ Xatolik yuz berdi: {e}")


@dp.callback_query(F.data.startswith("fuel_sheet:"), BotStates.FuelSheetSelect)
async def callback_fuel_sheet(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()

    data = await state.get_data()
    company = data.get("selected_company") or get_company(callback.from_user.id)
    if not company:
        await callback.message.edit_text("Iltimos, /start bosing va Load tanlang.")
        return
    sheet_names = data.get("fuel_sheet_names") or []
    fuel_entries = data.get("fuel_entries") or []
    if not sheet_names or not fuel_entries:
        await callback.message.edit_text("❌ Ma'lumotlar topilmadi. Qaytadan Fuel fayl yuboring.")
        await state.set_state(BotStates.Fuel)
        return

    idx_str = callback.data.replace("fuel_sheet:", "").strip()
    try:
        idx = int(idx_str)
    except ValueError:
        return
    if idx < 0 or idx >= len(sheet_names):
        return

    sheet_name = sheet_names[idx]

    await callback.message.edit_text("⏳ Kutib turing, natija tez orada chiqadi...")

    import re
    from datetime import datetime, date
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    from services.google_sheets import get_sheet_service

    sheet_service = get_sheet_service()

    # 1) Agar sheet nomida aniq sana oralig'i bo'lsa -> shu bilan ishlaymiz
    range_re = re.compile(r'(\d{1,2}\.\d{1,2})\s*-\s*(\d{1,2}\.\d{1,2})')
    year_m = re.search(r'(\d{4})', sheet_name)
    year = int(year_m.group(1)) if year_m else datetime.now().year

    m = range_re.search(sheet_name)
    if m:
        start_str, end_str = m.group(1), m.group(2)
        start_month, start_day = map(int, start_str.split('.'))
        end_month, end_day = map(int, end_str.split('.'))
        start_date = date(year, start_month, start_day)
        end_year = year
        if (end_month, end_day) < (start_month, start_day):
            end_year = year + 1
        end_date = date(end_year, end_month, end_day)

        # card_totals uchun filtr
        card_totals = {}
        for item in fuel_entries:
            try:
                item_date = datetime.fromisoformat(item["date"]).date()
            except Exception:
                continue
            if not (start_date <= item_date <= end_date):
                continue
            card = str(item.get("card", "")).strip()
            if not card:
                continue
            fuel_sum = float(item.get("fuel", 0.0) or 0.0)
            discount_sum = float(item.get("discount", 0.0) or 0.0)
            if card not in card_totals:
                card_totals[card] = [0.0, 0.0]
            card_totals[card][0] += fuel_sum
            card_totals[card][1] += discount_sum

        if not card_totals:
            await callback.message.edit_text(f"List <b>{sheet_name}</b> oralig'ida mos yozuv topilmadi.")
            await state.set_state(BotStates.Fuel)
            return

        # Nomdan sana oralig'i bo'lgan holatlarda E/F ustunlari odatda shunday (bizning eski taxmin)
        card_totals_tuple = {k: (v[0], v[1]) for k, v in card_totals.items()}
        updated, skipped, missing_count, missing_cards = sheet_service.update_fuel_toll_expenses(
            sheet_name,
            card_totals_tuple,
            fuel_col=5,  # E = Fuel after discount
            discount_col=6,  # F = Discount (Disc Amt dan, Toll Exp emas)
            company=company,
        )

        # Excel report (textda detalli matn yuborilmaydi)
        report_rows = []
        missing_set = set(str(x) for x in (missing_cards or []))
        week_label = f"{start_str}-{end_str}"

        for card, (fuel_sum, discount_sum) in card_totals_tuple.items():
            card_s = str(card)
            if (fuel_sum or 0) == 0 and (discount_sum or 0) == 0:
                continue
            status = "TOPILMADI" if card_s in missing_set else "TOPILDI"
            report_rows.append({
                "Sheet": sheet_name,
                "Week": week_label,
                "Card": card_s,
                "FuelSum": fuel_sum,
                "DiscountSum": discount_sum,
                "Status": status,
            })

        try:
            import pandas as pd
            import os
            import tempfile
            from aiogram.types import FSInputFile

            report_df = pd.DataFrame(report_rows)
            data_all = await state.get_data()
            src_file = data_all.get("fuel_filename") or "fuel.xlsx"
            base = os.path.splitext(str(src_file))[0]

            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
            tmp_path = tmp.name
            tmp.close()

            report_df.to_excel(tmp_path, index=False)
            # Status ustunini bo'yash: TOPILDI=green, TOPILMADI=red
            try:
                from openpyxl import load_workbook
                from openpyxl.styles import PatternFill

                wb = load_workbook(tmp_path)
                ws_rep = wb.active
                status_col_idx = None
                for c in range(1, ws_rep.max_column + 1):
                    if str(ws_rep.cell(row=1, column=c).value).strip() == "Status":
                        status_col_idx = c
                        break

                if status_col_idx:
                    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
                    for r in range(2, ws_rep.max_row + 1):
                        cell = ws_rep.cell(row=r, column=status_col_idx)
                        v = str(cell.value).strip().upper() if cell.value is not None else ""
                        if v == "TOPILDI":
                            cell.fill = green_fill
                        elif v == "TOPILMADI":
                            cell.fill = red_fill

                    wb.save(tmp_path)
            except Exception:
                pass
            await callback.message.answer_document(FSInputFile(tmp_path))
            os.remove(tmp_path)
        except Exception:
            pass

        await callback.message.edit_text("✅ Fuel yozildi. (Excel report yuborildi)")

        await state.set_state(BotStates.Fuel)
        return

    # 2) Agar sheet nomida oralig' bo'lmasa (masalan Owner Operators) -> sheet ichidan sana va ustunlarni topamiz
    ws = sheet_service.get_expenses_board(sheet_name, company)
    if not ws:
        await callback.message.edit_text(f"❌ {sheet_name} worksheet topilmadi.")
        await state.set_state(BotStates.Fuel)
        return

    # Faqat yuqori qismi: sana oralig'i va Fuel/Toll header joylari topiladi
    top_grid = ws.get("A1:Z6")
    # top_grid: list rows, col count = Z

    seg_re = re.compile(r'(\d{1,2}\.\d{1,2})\s*-\s*(\d{1,2}\.\d{1,2})')
    date_matches = []  # (start_date, end_date, start_col_idx)

    # sana oralig'i odatda row 1-3 da bo'ladi
    for r in range(min(len(top_grid), 10)):
        for c in range(min(26, len(top_grid[r]))):
            cell = top_grid[r][c]
            if cell is None:
                continue
            cell_s = str(cell)
            m2 = seg_re.search(cell_s)
            if not m2:
                continue
            # cell ichida yil bo'lishi mumkin (masalan: "2026 February/March 02.23-03.01")
            year_cell_m = re.search(r'(\d{4})', cell_s)
            year_cell = int(year_cell_m.group(1)) if year_cell_m else year
            start_str, end_str = m2.group(1), m2.group(2)
            start_month, start_day = map(int, start_str.split('.'))
            end_month, end_day = map(int, end_str.split('.'))

            s_date = date(year_cell, start_month, start_day)
            e_year = year_cell
            if (end_month, end_day) < (start_month, start_day):
                e_year = year_cell + 1
            e_date = date(e_year, end_month, end_day)
            # col index 0-based -> 1-based for gspread usage
            date_matches.append((s_date, e_date, c + 1))

    # Unikallashtirish
    unique = {}
    for s_date, e_date, c in date_matches:
        key = (s_date, e_date, c)
        unique[key] = True
    date_matches = sorted(list(unique.keys()), key=lambda x: x[2])

    # Fuel/Discount ustun topish (Disc Amt -> Discount, Toll Exp emas)
    fuel_cols = []
    discount_cols = []
    # Taxmin: Fuel/Discount header row 1-6 atrofida
    for r in range(min(6, len(top_grid))):
        for c in range(min(26, len(top_grid[r]))):
            cell = top_grid[r][c]
            if cell is None:
                continue
            cell_s = str(cell).lower()

            # "Fuel after discount", "Fuel Exp", "Fuel Expenses" kabi
            if "fuel" in cell_s and ("exp" in cell_s or "after" in cell_s):
                fuel_cols.append(c + 1)

            # "Discount", "Disc Amt" - Disc Amt (Q) shu ustunga yoziladi
            # "Fuel after discount" ni hisobga olmaslik: fuel bo'lmagan discount
            if "fuel" not in cell_s and ("discount" in cell_s or "disc" in cell_s):
                discount_cols.append(c + 1)

    # Agar topilmasa, fallback: E=Fuel, F=Discount
    if not fuel_cols:
        fuel_cols = [5]
    if not discount_cols:
        discount_cols = [6]

    # Har bir sana oralig'i uchun fuel/discount colni segmentga moslab tanlaymiz.
    # Segment: date_matches[i].start_col -> date_matches[i+1].start_col-1
    segments = []
    if date_matches:
        for i in range(len(date_matches)):
            start_date, end_date, start_col = date_matches[i]
            end_col = (date_matches[i + 1][2] - 1) if i + 1 < len(date_matches) else 26
            # segment ichida joylashgan fuel/discount header col'larini tanlaymiz
            f_col = next((fc for fc in fuel_cols if start_col <= fc <= end_col), None)
            d_col = next((dc for dc in discount_cols if start_col <= dc <= end_col), None)
            if f_col is None:
                f_col = fuel_cols[0]
            if d_col is None:
                d_col = discount_cols[0]
            segments.append({
                "label": f"{start_date.strftime('%m.%d')}-{end_date.strftime('%m.%d')}",
                "start_date": start_date,
                "end_date": end_date,
                "fuel_col": f_col,
                "discount_col": d_col,
            })
    else:
        # sana topilmasa - bitta umumiy segment
        segments = [{
            "label": sheet_name,
            "start_date": date(year, 1, 1),
            "end_date": date(year, 12, 31),
            "fuel_col": fuel_cols[0],
            "discount_col": discount_cols[0],
        }]

    # Auto-mapping: foydalanuvchi sana oralig'ini tanlamaydi,
    # xlsxdagi har bir sana o'z segmentiga tushib, shu haftaga yoziladi.
    card_totals_by_seg = {i: {} for i in range(len(segments))}
    matched_items = 0

    for item in fuel_entries:
        try:
            item_date = datetime.fromisoformat(item["date"]).date()
        except Exception:
            continue

        card = str(item.get("card", "")).strip()
        if not card:
            continue

        fuel_sum = float(item.get("fuel", 0.0) or 0.0)
        discount_sum = float(item.get("discount", 0.0) or 0.0)

        item_md = (item_date.month, item_date.day)
        assigned = False
        for i, seg in enumerate(segments):
            start_md = (seg["start_date"].month, seg["start_date"].day)
            end_md = (seg["end_date"].month, seg["end_date"].day)
            if start_md <= end_md:
                ok = start_md <= item_md <= end_md
            else:
                # year wrap: masalan 12.28-01.05
                ok = item_md >= start_md or item_md <= end_md

            if not ok:
                continue

            if card not in card_totals_by_seg[i]:
                card_totals_by_seg[i][card] = [0.0, 0.0]
            card_totals_by_seg[i][card][0] += fuel_sum
            card_totals_by_seg[i][card][1] += discount_sum
            assigned = True
            break

        if assigned:
            matched_items += 1

    total_updated = 0
    total_skipped = 0
    missing_cards_by_seg = {}

    for i, seg in enumerate(segments):
        card_totals = card_totals_by_seg.get(i) or {}
        if not card_totals:
            continue

        card_totals_tuple = {k: (v[0], v[1]) for k, v in card_totals.items()}
        updated, skipped, missing_count, missing_cards = sheet_service.update_fuel_toll_expenses(
            sheet_name,
            card_totals_tuple,
            fuel_col=seg["fuel_col"],
            discount_col=seg["discount_col"],
            company=company,
        )
        total_updated += updated
        total_skipped += skipped
        missing_cards_by_seg[i] = set(str(x) for x in (missing_cards or []))

    if matched_items == 0:
        await callback.message.edit_text(
            f"❌ {sheet_name} bo'yicha mos hafta topilmadi.\n\n"
            f"Fayldagi sana oralig'i: xlsx -> (tekshirilsin)."
        )
        await state.set_state(BotStates.Fuel)
        return

    # Excel report (TOPILDI/TOPILMADI va qaysi hafta)
    report_rows = []
    for i, seg in enumerate(segments):
        seg_cards = card_totals_by_seg.get(i) or {}
        if not seg_cards:
            continue
        seg_missing = missing_cards_by_seg.get(i) or set()
        for card, (fuel_sum, discount_sum) in seg_cards.items():
            card_s = str(card)
            if (fuel_sum or 0) == 0 and (discount_sum or 0) == 0:
                continue
            status = "TOPILMADI" if card_s in seg_missing else "TOPILDI"
            report_rows.append({
                "Sheet": sheet_name,
                "Week": seg.get("label"),
                "Card": card_s,
                "FuelSum": fuel_sum,
                "DiscountSum": discount_sum,
                "Status": status,
            })

    try:
        import pandas as pd
        import os
        import tempfile
        from aiogram.types import FSInputFile

        report_df = pd.DataFrame(report_rows)
        data_all = await state.get_data()
        src_file = data_all.get("fuel_filename") or "fuel.xlsx"
        base = os.path.splitext(str(src_file))[0]

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        tmp_path = tmp.name
        tmp.close()

        report_df.to_excel(tmp_path, index=False)
        # Status ustunini bo'yash: TOPILDI=green, TOPILMADI=red
        try:
            from openpyxl import load_workbook
            from openpyxl.styles import PatternFill

            wb = load_workbook(tmp_path)
            ws_rep = wb.active
            status_col_idx = None
            for c in range(1, ws_rep.max_column + 1):
                if str(ws_rep.cell(row=1, column=c).value).strip() == "Status":
                    status_col_idx = c
                    break

            if status_col_idx:
                green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
                for r in range(2, ws_rep.max_row + 1):
                    cell = ws_rep.cell(row=r, column=status_col_idx)
                    v = str(cell.value).strip().upper() if cell.value is not None else ""
                    if v == "TOPILDI":
                        cell.fill = green_fill
                    elif v == "TOPILMADI":
                        cell.fill = red_fill

                wb.save(tmp_path)
        except Exception:
            pass
        await callback.message.answer_document(FSInputFile(tmp_path))
        os.remove(tmp_path)
    except Exception:
        pass

    await callback.message.edit_text("✅ Fuel yozildi. (Excel report yuborildi)")

    await state.set_state(BotStates.Fuel)


@dp.callback_query(F.data.startswith("fuel_range:"), BotStates.FuelRangeSelect)
async def callback_fuel_range(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    from datetime import datetime
    data = await state.get_data()
    company = data.get("selected_company") or get_company(callback.from_user.id)
    if not company:
        await callback.message.edit_text("Iltimos, /start bosing va Load tanlang.")
        return
    segments = data.get("fuel_segments") or []
    fuel_entries = data.get("fuel_entries") or []
    if not segments or not fuel_entries:
        await callback.message.edit_text("❌ Ma'lumotlar yo'q. Qaytadan Fuel fayl yuboring.")
        await state.set_state(BotStates.Fuel)
        return

    idx_str = callback.data.replace("fuel_range:", "").strip()
    try:
        idx = int(idx_str)
    except ValueError:
        return
    if idx < 0 or idx >= len(segments):
        return

    seg = segments[idx]
    sheet_name = data.get("fuel_selected_sheet") or ""
    if not sheet_name:
        await callback.message.edit_text("❌ Tanlangan sheet nomi topilmadi. Qaytadan ishlang.")
        await state.set_state(BotStates.Fuel)
        return

    await callback.message.edit_text("⏳ Kutib turing, natija tez orada chiqadi...")

    # Filter faqat shu oralig'idagi yozuvlar.
    # Yil noto'g'ri chiqsa ham muammo bo'lmasligi uchun month/day bo'yicha taqqoslaymiz.
    card_totals = {}
    start_md = (seg["start_date"].month, seg["start_date"].day)
    end_md = (seg["end_date"].month, seg["end_date"].day)

    def in_range_md(d):
        md = (d.month, d.day)
        if start_md <= end_md:
            return start_md <= md <= end_md
        # Year wrap (masalan 12.28-01.05)
        return md >= start_md or md <= end_md

    for item in fuel_entries:
        try:
            item_date = datetime.fromisoformat(item["date"]).date()
        except Exception:
            continue
        if not in_range_md(item_date):
            continue
        card = str(item.get("card", "")).strip()
        if not card:
            continue
        fuel_sum = float(item.get("fuel", 0.0) or 0.0)
        discount_sum = float(item.get("discount", 0.0) or 0.0)
        if card not in card_totals:
            card_totals[card] = [0.0, 0.0]
        card_totals[card][0] += fuel_sum
        card_totals[card][1] += discount_sum

    if not card_totals:
        await callback.message.edit_text(f"List <b>{sheet_name}</b> oralig'ida mos yozuv topilmadi.")
        await state.set_state(BotStates.Fuel)
        return

    from services.google_sheets import get_sheet_service
    sheet_service = get_sheet_service()

    card_totals_tuple = {k: (v[0], v[1]) for k, v in card_totals.items()}
    updated, skipped, missing_count, missing_cards = sheet_service.update_fuel_toll_expenses(
        sheet_name,
        card_totals_tuple,
        fuel_col=seg["fuel_col"],
        discount_col=seg["discount_col"],
        company=company,
    )

    report_rows = []
    missing_set = set(str(x) for x in (missing_cards or []))
    week_label = seg.get("label")
    for card, (fuel_sum, discount_sum) in card_totals_tuple.items():  # fuel_range
        card_s = str(card)
        if (fuel_sum or 0) == 0 and (discount_sum or 0) == 0:
            continue
        status = "TOPILMADI" if card_s in missing_set else "TOPILDI"
        report_rows.append({
            "Sheet": sheet_name,
            "Week": week_label,
            "Card": card_s,
            "FuelSum": fuel_sum,
            "DiscountSum": discount_sum,
            "Status": status,
        })

    try:
        import pandas as pd
        import os
        import tempfile
        from aiogram.types import FSInputFile

        report_df = pd.DataFrame(report_rows)
        data_all = await state.get_data()
        src_file = data_all.get("fuel_filename") or "fuel.xlsx"
        base = os.path.splitext(str(src_file))[0]

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        tmp_path = tmp.name
        tmp.close()

        report_df.to_excel(tmp_path, index=False)
        # Status ustunini bo'yash: TOPILDI=green, TOPILMADI=red
        try:
            from openpyxl import load_workbook
            from openpyxl.styles import PatternFill

            wb = load_workbook(tmp_path)
            ws_rep = wb.active
            status_col_idx = None
            for c in range(1, ws_rep.max_column + 1):
                if str(ws_rep.cell(row=1, column=c).value).strip() == "Status":
                    status_col_idx = c
                    break

            if status_col_idx:
                green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
                red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
                for r in range(2, ws_rep.max_row + 1):
                    cell = ws_rep.cell(row=r, column=status_col_idx)
                    v = str(cell.value).strip().upper() if cell.value is not None else ""
                    if v == "TOPILDI":
                        cell.fill = green_fill
                    elif v == "TOPILMADI":
                        cell.fill = red_fill

                wb.save(tmp_path)
        except Exception:
            pass
        await callback.message.answer_document(FSInputFile(tmp_path))
        os.remove(tmp_path)
    except Exception:
        pass

    await callback.message.edit_text("✅ Fuel yozildi. (Excel report yuborildi)")
    await state.set_state(BotStates.Fuel)


@dp.callback_query(F.data.startswith("toll_sheet:"), BotStates.TollSheetSelect)
async def callback_toll_sheet(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    import re
    from datetime import datetime, date

    data = await state.get_data()
    company = data.get("selected_company") or get_company(callback.from_user.id)
    if not company:
        await callback.message.edit_text("Iltimos, /start bosing va Load tanlang.")
        return
    sheet_names = data.get("toll_sheet_names") or []
    toll_entries = data.get("toll_entries") or []
    if not sheet_names or not toll_entries:
        await callback.message.edit_text("❌ Ma'lumotlar topilmadi. Qaytadan Toll fayl yuboring.")
        await state.set_state(BotStates.Toll)
        return

    idx_str = callback.data.replace("toll_sheet:", "").strip()
    try:
        idx = int(idx_str)
    except ValueError:
        return
    if idx < 0 or idx >= len(sheet_names):
        return

    sheet_name = sheet_names[idx]
    await callback.message.edit_text("⏳ Kutib turing, natija tez orada chiqadi...")

    from services.google_sheets import get_sheet_service
    sheet_service = get_sheet_service()

    ws = sheet_service.get_expenses_board(sheet_name, company)
    if not ws:
        await callback.message.edit_text(f"❌ {sheet_name} worksheet topilmadi.")
        await state.set_state(BotStates.Toll)
        return

    top_grid = ws.get("A1:Z6")
    seg_re = re.compile(r'(\d{1,2}\.\d{1,2})\s*-\s*(\d{1,2}\.\d{1,2})')
    year = datetime.now().year
    date_matches = []
    for r in range(min(len(top_grid), 10)):
        for c in range(min(26, len(top_grid[r]) if top_grid[r] else 0)):
            cell = top_grid[r][c] if c < len(top_grid[r]) else None
            if cell is None:
                continue
            cell_s = str(cell)
            m2 = seg_re.search(cell_s)
            if not m2:
                continue
            year_m = re.search(r'(\d{4})', cell_s)
            year_cell = int(year_m.group(1)) if year_m else year
            start_str, end_str = m2.group(1), m2.group(2)
            start_month, start_day = map(int, start_str.split('.'))
            end_month, end_day = map(int, end_str.split('.'))
            s_date = date(year_cell, start_month, start_day)
            e_year = year_cell
            if (end_month, end_day) < (start_month, start_day):
                e_year = year_cell + 1
            e_date = date(e_year, end_month, end_day)
            date_matches.append((s_date, e_date, c + 1))

    unique = {}
    for s_date, e_date, c in date_matches:
        unique[(s_date, e_date, c)] = True
    date_matches = sorted(list(unique.keys()), key=lambda x: x[2])

    toll_cols = []
    for r in range(min(6, len(top_grid))):
        row_data = top_grid[r] if r < len(top_grid) else []
        for c in range(min(26, len(row_data))):
            cell = row_data[c] if c < len(row_data) else None
            if cell is None:
                continue
            cell_s = str(cell).lower()
            if "toll" in cell_s and ("exp" in cell_s or "expenses" in cell_s):
                toll_cols.append(c + 1)

    if not toll_cols:
        toll_cols = [7]

    segments = []
    if date_matches:
        for i in range(len(date_matches)):
            start_date, end_date, start_col = date_matches[i]
            end_col = (date_matches[i + 1][2] - 1) if i + 1 < len(date_matches) else 26
            t_col = next((tc for tc in toll_cols if start_col <= tc <= end_col), toll_cols[0])
            segments.append({
                "label": f"{start_date.strftime('%m.%d')}-{end_date.strftime('%m.%d')}",
                "start_date": start_date,
                "end_date": end_date,
                "toll_col": t_col,
            })
    else:
        segments = [{
            "label": sheet_name,
            "start_date": date(year, 1, 1),
            "end_date": date(year, 12, 31),
            "toll_col": toll_cols[0],
        }]

    transponder_totals_by_seg = {i: {} for i in range(len(segments))}
    matched = 0
    for item in toll_entries:
        try:
            item_date = datetime.fromisoformat(item["date"]).date()
        except Exception:
            continue
        transponder = str(item.get("transponder", "")).strip()
        if not transponder:
            continue
        toll_sum = float(item.get("toll", 0.0) or 0.0)
        item_md = (item_date.month, item_date.day)
        for i, seg in enumerate(segments):
            start_md = (seg["start_date"].month, seg["start_date"].day)
            end_md = (seg["end_date"].month, seg["end_date"].day)
            if start_md <= end_md:
                ok = start_md <= item_md <= end_md
            else:
                ok = item_md >= start_md or item_md <= end_md
            if ok:
                if transponder not in transponder_totals_by_seg[i]:
                    transponder_totals_by_seg[i][transponder] = 0.0
                transponder_totals_by_seg[i][transponder] += toll_sum
                matched += 1
                break

    if matched == 0:
        await callback.message.edit_text("❌ Mos hafta topilmadi. Faylni tekshiring.")
        await state.set_state(BotStates.Toll)
        return

    total_updated = 0
    missing_by_seg = {}
    for i, seg in enumerate(segments):
        totals = transponder_totals_by_seg.get(i) or {}
        if not totals:
            continue
        updated, skipped, _, missing = sheet_service.update_toll_expenses(
            sheet_name,
            totals,
            toll_col=seg["toll_col"],
            transponder_col=4,
            company=company,
        )
        total_updated += updated
        missing_by_seg[i] = set(str(x) for x in (missing or []))

    # Excel report: Transponder, Week, TollSum, Status (TOPILDI=yashil, TOPILMADI=qizil)
    report_rows = []
    for i, seg in enumerate(segments):
        totals = transponder_totals_by_seg.get(i) or {}
        seg_missing = missing_by_seg.get(i) or set()
        for transponder, toll_sum in totals.items():
            trans_s = str(transponder)
            status = "TOPILMADI" if trans_s in seg_missing else "TOPILDI"
            report_rows.append({
                "Sheet": sheet_name,
                "Week": seg.get("label", ""),
                "Transponder": trans_s,
                "TollSum": toll_sum,
                "Status": status,
            })

    try:
        import pandas as pd
        import os
        import tempfile
        from aiogram.types import FSInputFile

        report_df = pd.DataFrame(report_rows)
        data_all = await state.get_data()
        src_file = data_all.get("toll_filename") or "toll.xlsx"

        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        tmp_path = tmp.name
        tmp.close()

        report_df.to_excel(tmp_path, index=False)

        # Status ustunini bo'yash: TOPILDI=to'q yashil, TOPILMADI=to'q qizil
        try:
            from openpyxl import load_workbook
            from openpyxl.styles import PatternFill

            wb = load_workbook(tmp_path)
            ws_rep = wb.active
            status_col_idx = None
            for c in range(1, ws_rep.max_column + 1):
                if str(ws_rep.cell(row=1, column=c).value).strip() == "Status":
                    status_col_idx = c
                    break

            if status_col_idx:
                green_fill = PatternFill(start_color="2E7D32", end_color="2E7D32", fill_type="solid")  # to'q yashil
                red_fill = PatternFill(start_color="C62828", end_color="C62828", fill_type="solid")   # to'q qizil
                for r in range(2, ws_rep.max_row + 1):
                    cell = ws_rep.cell(row=r, column=status_col_idx)
                    v = str(cell.value).strip().upper() if cell.value is not None else ""
                    if v == "TOPILDI":
                        cell.fill = green_fill
                    elif v == "TOPILMADI":
                        cell.fill = red_fill

                wb.save(tmp_path)
        except Exception:
            pass

        await callback.message.answer_document(FSInputFile(tmp_path))
        os.remove(tmp_path)
    except Exception:
        pass

    await callback.message.edit_text("✅ Toll yozildi. Excel report yuborildi.")
    await state.set_state(BotStates.Toll)
