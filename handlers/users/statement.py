import os
import re

import pandas as pd
from aiogram import F, types
from aiogram.fsm.context import FSMContext
from openpyxl.styles import PatternFill

import config
from keyboards.default.main_menu import get_load_select_menu, get_main_menu
from keyboards.default.statement_menu import statement_menu
from loader import bot, dp
from services.company_driver_pdf import parse_company_driver_settlement_pdf
from services.excel_parser import ExcelParser
from services.google_sheets import get_sheet_service
from states.bot_states import BotStates
from utils.access_control import is_super_admin
from utils.company_storage import get_company


def _load_board_hint(company: str) -> str:
    """Qaysi kompaniya Load Board (Google Sheet) ishlatilayotganini ko'rsatadi."""
    return (
        f"📋 <b>Load Board:</b> {company}\n"
        f"<i>Tekshiruv shu kompaniya uchun .env dagi LOAD sheet kaliti bo'yicha.</i>\n\n"
    )


def _drivers_match(pdf_driver: str, sheet_driver: str) -> bool:
    p = re.sub(r"\s+", " ", (pdf_driver or "").strip().lower())
    s = re.sub(r"\s+", " ", (sheet_driver or "").strip().lower())
    if not p or not s:
        return False
    if p == s or p in s or s in p:
        return True
    wp, ws = set(p.split()), set(s.split())
    return len(wp & ws) >= 2


def _pdf_sheet_id_match(sheet_service, pdf_tid, sheet_load_raw) -> str:
    """PDF trip ID va sheet D ustunidagi LOAD # bir xil (normalizatsiyadan keyin)mi."""
    if not pdf_tid or sheet_load_raw is None or str(sheet_load_raw).strip() == "":
        return "-"
    a = sheet_service._normalize_load_num(pdf_tid)
    b = sheet_service._normalize_load_num(sheet_load_raw)
    if not a or not b:
        return "-"
    return "ha" if a == b else "yo'q"


def _deny_if_not_super_admin(user_id: int) -> bool:
    return not is_super_admin(user_id)


@dp.message(F.text == "📊 Statement Check")
async def enter_statement(message: types.Message, state: FSMContext):
    if _deny_if_not_super_admin(message.from_user.id):
        return
    company = get_company(message.from_user.id)
    if not company:
        await message.answer("Iltimos, avval Load tanlang:", reply_markup=get_load_select_menu(message.from_user.id))
        return
    await state.set_state(BotStates.Statement)
    await message.answer(
        _load_board_hint(company)
        + "Statement Check bo'limi.\nKim uchun tekshiruvni amalga oshiramiz?",
        reply_markup=statement_menu,
        parse_mode="HTML",
    )

@dp.message(F.text == "⬅️ Back (Main Menu)", BotStates.Statement)
async def back_statement(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Asosiy menyu:", reply_markup=get_main_menu(message.from_user.id))


@dp.message(F.text == "⬅️ Back (Main Menu)", BotStates.StatementCompanyDriverPdf)
async def back_company_driver_pdf(message: types.Message, state: FSMContext):
    await state.set_state(BotStates.Statement)
    await message.answer("Statement Check bo'limi.", reply_markup=statement_menu)


def _statement_fayl_yuklash_texts():
    """Eski klaviaturalarda 📥, yangisida 📤 — ikkalasini ham qabul qilamiz."""
    return ("📤 Fayl yuklash", "📥 Fayl yuklash")


@dp.message(F.text == "🏢 Company Driver")
async def ask_company_driver_pdf(message: types.Message, state: FSMContext):
    """
    Statement FSM yo'qolganda ham ishlashi kerak (MemoryStorage, bot qayta ishga tushganda).
    Holat filterisiz — kompaniya (Load) tanlangan bo'lishi kerak.
    """
    if _deny_if_not_super_admin(message.from_user.id):
        return
    company = get_company(message.from_user.id)
    if not company:
        await message.answer(
            "Iltimos, avval Load tanlang:",
            reply_markup=get_load_select_menu(message.from_user.id),
        )
        return
    await state.set_state(BotStates.StatementCompanyDriverPdf)
    await message.answer(
        _load_board_hint(company)
        + "🏢 <b>Company Driver</b> settlement tekshiruvi.\n\n"
        "Iltimos, <b>PDF</b> faylini yuboring.\n"
        "Fayldan haydovchi ismi, foiz (Percent), ish davri va Trips jadvalidagi "
        "load/trip ID hamda Rate (Gross) o'qiladi, keyin <b>yuqoridagi kompaniya</b> Load Board bilan solishtiriladi.",
        parse_mode="HTML",
        reply_markup=statement_menu,
    )


@dp.message(F.text.in_(_statement_fayl_yuklash_texts()), BotStates.StatementCompanyDriverPdf)
async def ask_statement_file_from_pdf_flow(message: types.Message, state: FSMContext):
    if _deny_if_not_super_admin(message.from_user.id):
        await state.clear()
        return
    await state.set_state(BotStates.Statement)
    await message.answer("Statement Excel (xlsx, xls) faylini yuboring.")


@dp.message(F.text.in_(_statement_fayl_yuklash_texts()), BotStates.Statement)
async def ask_statement_file(message: types.Message):
    if _deny_if_not_super_admin(message.from_user.id):
        return
    await message.answer("Statement Excel (xlsx, xls) faylini yuboring.")

@dp.message(F.document, BotStates.Statement)
async def handle_statement_doc(message: types.Message, state: FSMContext):
    if _deny_if_not_super_admin(message.from_user.id):
        await state.clear()
        return
    company = get_company(message.from_user.id)
    if not company:
        await message.answer("Iltimos, avval Load tanlang:", reply_markup=get_load_select_menu(message.from_user.id))
        return

    document = message.document
    file_id = document.file_id
    file_name = document.file_name

    if not (file_name.endswith('.xlsx') or file_name.endswith('.xls')):
        await message.answer("Iltimos, faqat Excel (xlsx, xls) fayl yuklang.")
        return
    
    await message.answer(
        f"Fayl qabul qilindi. <b>{company}</b> Load Board bo'yicha solishtirish boshlanmoqda... ⏳",
        parse_mode="HTML",
    )
    
    try:
        file = await bot.get_file(file_id)
        file_content = await bot.download_file(file.file_path)
        content_bytes = file_content.read()
        
        # Parse STATEMENT data
        # We reuse parse_invoice but it might need to be more flexible if columns differ.
        # Assuming Statement has 'Load #' and at least 'Total' or 'Amount'.
        
        parsed_data = ExcelParser.parse_invoice(content_bytes)
        
        if not parsed_data:
             await message.answer("Faylni o'qib bo'lmadi. 'Load #' ustuni borligiga ishonch hosil qiling.")
             return

        try:
            sheet_service = get_sheet_service()
        except Exception as e:
            err = str(e)
            if "429" in err or "Quota" in err or "quota" in err:
                await message.answer(
                    "⚠️ Google Sheets daqiqalik limiti. 1–3 daqiqa kutib qayta yuboring."
                )
            else:
                await message.answer(f"Xatolik: {e}")
            return

        results = []
        match_count = 0
        mismatch_count = 0
        not_found_count = 0
        
        sheet_cache = {}
        
        status_msg = await message.answer(
            f"📋 {company} | Jarayon: 0/{len(parsed_data)}"
        )
        
        for idx, row in enumerate(parsed_data):
            load_num = row['load_number']
            stmt_amount = row['amount']
            date_obj = row.get('date')
            
            if idx % 5 == 0:
                 try: await status_msg.edit_text(f"📋 {company} | Jarayon: {idx}/{len(parsed_data)}")
                 except: pass

            sheet_name = None
            if date_obj:
                if date_obj not in sheet_cache:
                    sheet_name = sheet_service.get_sheet_by_date(date_obj, company=company)
                    sheet_cache[date_obj] = sheet_name
                else:
                    sheet_name = sheet_cache[date_obj]
            
            # Agar sana bo'lmasa, barcha oxirgi sheetlarni qidirish qiyin. 
            # Hozircha STOP. Sana bo'lishi shart deb hisoblaymiz yoki "Date Not Found"
            
            sheet_status = "SHEET NOT FOUND"
            diff = 0
            sheet_amount = 0
            comment = ""
            
            if sheet_name:
                row_num = sheet_service.find_load_row(load_num, sheet_name, company=company)
                
                if row_num:
                    details = sheet_service.get_load_details(row_num, sheet_name, company=company)
                    if details:
                        # Compare logic
                        # Statement amount usually matches Invoiced OR Broker Paid
                        # Let's compare with Invoiced Amount (Col P) as primary
                        
                        sheet_invoiced = details['invoiced']
                        sheet_paid = details['broker_paid']
                        
                        # Qaysi biri bilan solishtirishni aniqlash qiyin bo'lishi mumkin.
                        # Odatda Statement bu bizga to'lanishi kerak bo'lgan pul (Factoring Statement)
                        # yoki Driverga to'lanadigan (Settlement).
                        # "Statement Check" deganda odatda Factoring kompaniyasi yuborgan statementni
                        # bizning Load Boarddagi Invoiced Amount bilan solishtirish tushuniladi.
                        
                        # COMPARE WITH INVOICED (Col P)
                        sheet_amount = sheet_invoiced
                        diff = sheet_amount - stmt_amount
                        
                        if abs(diff) < 0.01:
                            sheet_status = "MATCH"
                            match_count += 1
                        else:
                            sheet_status = "MISMATCH"
                            mismatch_count += 1
                            comment = f"Sheet: {sheet_amount} | Stmt: {stmt_amount}"
                    else:
                        sheet_status = "ERROR READING ROW"
                else:
                    sheet_status = "LOAD NOT FOUND"
                    not_found_count += 1
            else:
                sheet_status = "SHEET NOT FOUND (NO DATE)"
                not_found_count += 1
                
            results.append({
                "Kompaniya (Load Board)": company,
                "Load #": load_num,
                "Statement Amount": stmt_amount,
                "Sheet Amount": sheet_amount,
                "Diff": diff,
                "Status": sheet_status,
                "Date": date_obj,
                "Comment": comment
            })
            
        await status_msg.delete()
        
        # Report gen
        df = pd.DataFrame(results)
        report_name = f"Statement_Result_{file_name}"
        df.to_excel(report_name, index=False)
        
        await message.answer(
            f"🏁 Solishtirish yakunlandi (<b>{company}</b> Load Board).\n"
            f"✅ Match: {match_count}\n"
            f"❌ Mismatch: {mismatch_count}\n"
            f"❓ Not Found: {not_found_count}",
            parse_mode="HTML",
        )
                             
        await message.answer_document(types.FSInputFile(report_name))
        os.remove(report_name)
        
    except Exception as e:
        err = str(e)
        if "429" in err or "Quota" in err or "quota" in err:
            await message.answer(
                "\u26a0\ufe0f Google Sheets daqiqalik limiti. 1–3 daqiqa kutib qayta yuboring."
            )
        else:
            await message.answer(f"Xatolik: {e}")


@dp.message(F.document, BotStates.StatementCompanyDriverPdf)
async def handle_company_driver_pdf(message: types.Message, state: FSMContext):
    if _deny_if_not_super_admin(message.from_user.id):
        await state.clear()
        return
    company = get_company(message.from_user.id)
    if not company:
        await message.answer(
            "Iltimos, avval Load tanlang:",
            reply_markup=get_load_select_menu(message.from_user.id),
        )
        return

    document = message.document
    file_name = (document.file_name or "").lower()
    mime = (document.mime_type or "").lower()
    if not file_name.endswith(".pdf") and mime != "application/pdf":
        await message.answer("Iltimos, faqat <b>PDF</b> fayl yuboring.", parse_mode="HTML")
        return

    await message.answer(
        f"PDF qabul qilindi. <b>{company}</b> Load Board bo'yicha o'qish va solishtirish... ⏳",
        parse_mode="HTML",
    )

    try:
        file = await bot.get_file(document.file_id)
        file_content = await bot.download_file(file.file_path)
        content_bytes = file_content.read()

        parsed = parse_company_driver_settlement_pdf(content_bytes)
        warnings = parsed.get("parse_warnings") or []

        try:
            sheet_service = get_sheet_service()
        except Exception as e:
            err = str(e)
            if "429" in err or "Quota" in err or "quota" in err:
                await message.answer(
                    "⚠️ Google Sheets daqiqalik limiti. 1–3 daqiqa kutib qayta yuboring."
                )
            else:
                await message.answer(f"Xatolik: {e}")
            return

        anchor = parsed.get("anchor_date")
        primary_sheet = None
        if anchor:
            primary_sheet = sheet_service.get_sheet_by_date(anchor, company=company)
        if not primary_sheet:
            weeks = sheet_service.get_last_n_week_sheets(10, company=company)
            primary_sheet = weeks[0] if weeks else None

        fallback_sheets = sheet_service.get_last_n_week_sheets(14, company=company)
        sheets_to_index = list(dict.fromkeys([s for s in ([primary_sheet] if primary_sheet else []) + list(fallback_sheets) if s]))
        load_index_cache: dict[str, dict] = {}
        for sn in sheets_to_index:
            load_index_cache[sn] = sheet_service.get_load_row_index(sn, company=company)

        results = []
        ok_n = bad_n = miss_n = 0

        for trip in parsed.get("trips") or []:
            tid = trip.get("trip_id")
            pdf_rate = trip.get("rate_gross")
            row_num = None
            sn = None
            key = sheet_service._normalize_load_num(tid) if tid else ""

            if primary_sheet and key:
                idx = load_index_cache.get(primary_sheet) or {}
                if key in idx:
                    row_num, sn = idx[key], primary_sheet

            if not row_num and key:
                for fb in fallback_sheets:
                    idx = load_index_cache.get(fb) or {}
                    if key in idx:
                        row_num, sn = idx[key], fb
                        break

            sheet_load = ""
            sheet_rate = None
            rate_ok = False
            natija = ""
            sabab = ""

            if not sn or not row_num:
                natija = "MOS KELMADI"
                miss_n += 1
                sabab = "LOAD ID sheetda topilmadi"
            else:
                fields = sheet_service.get_settlement_compare_fields(row_num, sn, company=company)
                if not fields:
                    natija = "MOS KELMADI"
                    bad_n += 1
                    sabab = "Sheet qatori o'qilmadi"
                else:
                    sheet_load = fields.get("load_number")
                    sheet_rate = fields.get("rate")
                    try:
                        rate_ok = abs(float(sheet_rate or 0) - float(pdf_rate or 0)) < 0.02
                    except (TypeError, ValueError):
                        rate_ok = False

                    id_mos = _pdf_sheet_id_match(sheet_service, tid, sheet_load) == "ha"
                    if id_mos and rate_ok:
                        natija = "MOS KELDI"
                        ok_n += 1
                    else:
                        natija = "MOS KELMADI"
                        bad_n += 1
                        bits = []
                        if not id_mos:
                            bits.append("LOAD ID mos emas")
                        if not rate_ok:
                            bits.append("RATE mos emas")
                        sabab = ", ".join(bits)

            results.append(
                {
                    "PDF Load ID": tid or "-",
                    "PDF Rate (Gross)": pdf_rate,
                    "Sheet List": sn or "(topilmadi)",
                    "Sheet Row": row_num or "-",
                    "Sheet Load ID": sheet_load if row_num else "-",
                    f"Sheet Rate (col {config.LOAD_BOARD_RATE_COL})": sheet_rate if row_num else "-",
                    "Natija": natija,
                    "Moslik": "✅" if natija == "MOS KELDI" else "❌",
                    "Mos kelmagan joy": sabab or "-",
                }
            )

        detail = pd.DataFrame(results)
        base = re.sub(r'[<>:"/\\|?*]', "_", document.file_name or "report").strip() or "report"
        report_name = f"CompanyDriver_check_{base}.xlsx"
        if not report_name.lower().endswith(".xlsx"):
            report_name += ".xlsx"

        with pd.ExcelWriter(report_name, engine="openpyxl") as writer:
            detail.to_excel(writer, sheet_name="Solishtirish", index=False)
            ws = writer.book["Solishtirish"]
            green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
            # Natija ustuni G (header bilan 1-qator, data 2-qatordan)
            for r in range(2, ws.max_row + 1):
                natija_val = str(ws[f"G{r}"].value or "").strip().upper()
                if natija_val == "MOS KELDI":
                    for c in range(1, ws.max_column + 1):
                        ws.cell(row=r, column=c).fill = green_fill

        warn_text = "\n".join(f"⚠️ {w}" for w in warnings) if warnings else ""
        if warn_text:
            await message.answer(warn_text)

        await message.answer(
            f"🏁 Tekshiruv tugadi — <b>{company}</b> Load Board.\n"
            f"✅ To'g'ri: {ok_n}\n"
            f"❌ Noto'g'ri: {bad_n}\n"
            f"❓ Topilmadi: {miss_n}\n"
            f"📋 Asosiy list: {primary_sheet or '-'}",
            parse_mode="HTML",
        )
        await message.answer_document(types.FSInputFile(report_name))
        os.remove(report_name)
        await state.set_state(BotStates.Statement)

    except Exception as e:
        err = str(e)
        if "429" in err or "Quota" in err or "quota" in err:
            await message.answer(
                "\u26a0\ufe0f Google Sheets daqiqalik limiti. 1–3 daqiqa kutib qayta yuboring."
            )
        else:
            await message.answer(f"Xatolik: {e}")
