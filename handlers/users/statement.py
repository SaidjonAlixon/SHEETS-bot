from aiogram import types, F
from aiogram.fsm.context import FSMContext
from loader import dp, bot
from keyboards.default.statement_menu import statement_menu
from keyboards.default.main_menu import get_main_menu, get_load_select_menu
from services.excel_parser import ExcelParser
from services.google_sheets import get_sheet_service
from states.bot_states import BotStates
from utils.company_storage import get_company
import pandas as pd
import os


@dp.message(F.text == "📊 Statement Check")
async def enter_statement(message: types.Message, state: FSMContext):
    from utils.access_control import is_admin
    if not is_admin(message.from_user.id):
        await message.answer("Sizda ruxsat yo'q.", reply_markup=get_main_menu(message.from_user.id))
        return
    company = get_company(message.from_user.id)
    if not company:
        await message.answer("Iltimos, avval Load tanlang:", reply_markup=get_load_select_menu(message.from_user.id))
        return
    await state.set_state(BotStates.Statement)
    await message.answer("Statement Check bo'limi.\n"
                         "Kim uchun tekshiruvni amalga oshiramiz?", reply_markup=statement_menu)

@dp.message(F.text == "⬅️ Back (Main Menu)", BotStates.Statement)
async def back_statement(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Asosiy menyu:", reply_markup=get_main_menu(message.from_user.id))

@dp.message(F.text == "📤 Fayl yuklash", BotStates.Statement)
async def ask_statement_file(message: types.Message):
    await message.answer("Statement Excel (xlsx, xls) faylini yuboring.")

@dp.message(F.document, BotStates.Statement)
async def handle_statement_doc(message: types.Message, state: FSMContext):
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
    
    await message.answer("Fayl qabul qilindi. Solishtirish boshlanmoqda... ⏳")
    
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
            if "429" in str(e):
                await message.answer("⚠️ Google Sheets limiti tugadi. 1–2 daqiqa kutib qayta yuboring.")
            else:
                await message.answer(f"Xatolik: {e}")
            return

        results = []
        match_count = 0
        mismatch_count = 0
        not_found_count = 0
        
        sheet_cache = {}
        
        status_msg = await message.answer(f"Jarayon: 0/{len(parsed_data)}")
        
        for idx, row in enumerate(parsed_data):
            load_num = row['load_number']
            stmt_amount = row['amount']
            date_obj = row.get('date')
            
            if idx % 5 == 0:
                 try: await status_msg.edit_text(f"Jarayon: {idx}/{len(parsed_data)}")
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
        
        await message.answer(f"🏁 Solishtirish yakunlandi.\n"
                             f"✅ Match: {match_count}\n"
                             f"❌ Mismatch: {mismatch_count}\n"
                             f"❓ Not Found: {not_found_count}")
                             
        await message.answer_document(types.FSInputFile(report_name))
        os.remove(report_name)
        
    except Exception as e:
        await message.answer(f"Xatolik: {e}")
