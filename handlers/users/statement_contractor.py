import asyncio
import os
import re
from collections import defaultdict

import pandas as pd
from aiogram import F, types
from aiogram.fsm.context import FSMContext
from openpyxl.styles import PatternFill

from handlers.users.statement import (
    _drivers_match,
    _extract_sheet_segments,
    _find_sheet_by_alias,
    _is_internal_trip_number,
    _load_board_hint,
    _money_eq,
    _pdf_trip_ids_match_sheet_cell,
)
from keyboards.default.main_menu import get_load_select_menu
from keyboards.default.statement_menu import statement_menu
from loader import bot, dp
from services.contractor_pdf import parse_contractor_settlement_pdf_ai
from services.google_sheets import get_sheet_service
from states.bot_states import BotStates
from utils.company_storage import get_company

CONTRACTOR_PROCESS_LOCK = asyncio.Lock()


@dp.message(F.text == "👷 Contractor")
async def ask_contractor_pdf(message: types.Message, state: FSMContext):
    company = get_company(message.from_user.id)
    if not company:
        await message.answer(
            "Iltimos, avval Load tanlang:",
            reply_markup=get_load_select_menu(message.from_user.id),
        )
        return
    await state.set_state(BotStates.StatementContractorPdf)
    await message.answer(
        _load_board_hint(company)
        + "👷 <b>Contractor</b> settlement tekshiruvi.\n\n"
        "Iltimos, <b>PDF</b> faylini yuboring.\n"
        "Tekshiruvlar 🚚 Owner Operator bilan bir xil: Load/Fuel/Toll.",
        parse_mode="HTML",
        reply_markup=statement_menu,
    )


@dp.message(F.text == "⬅️ Back (Main Menu)", BotStates.StatementContractorPdf)
async def back_contractor_pdf(message: types.Message, state: FSMContext):
    await state.set_state(BotStates.Statement)
    await message.answer("Statement Check bo'limi.", reply_markup=statement_menu)


@dp.message(F.document, BotStates.StatementContractorPdf)
async def handle_contractor_pdf(message: types.Message, state: FSMContext):
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

    if CONTRACTOR_PROCESS_LOCK.locked():
        await message.answer("📥 PDF navbatga qo'shildi. Oldingi fayl(lar) tugagach ketma-ket tekshiriladi.")

    await CONTRACTOR_PROCESS_LOCK.acquire()
    try:
        await message.answer(
            f"PDF qabul qilindi. <b>{company}</b> bo'yicha Contractor tekshiruv... ⏳",
            parse_mode="HTML",
        )

        file = await bot.get_file(document.file_id)
        file_content = await bot.download_file(file.file_path)
        content_bytes = file_content.read()
        parsed = parse_contractor_settlement_pdf_ai(content_bytes)
        warnings = parsed.get("parse_warnings") or []

        sheet_service = get_sheet_service()
        anchor = parsed.get("anchor_date")
        primary_sheet = sheet_service.get_sheet_by_date(anchor, company=company) if anchor else None
        fallback_sheets = sheet_service.get_last_n_week_sheets(14, company=company)
        sheets_to_index = list(
            dict.fromkeys([s for s in ([primary_sheet] if primary_sheet else []) + list(fallback_sheets) if s])
        )

        load_index_cache: dict[str, dict] = {}
        for sn in sheets_to_index:
            load_index_cache[sn] = sheet_service.get_load_row_index(sn, company=company)

        load_rows = []
        load_ok = load_bad = load_miss = 0

        for d in parsed.get("drivers") or []:
            driver_name = str(d.get("driver_name") or "").strip()
            grouped: dict[tuple[str, int], list[dict]] = defaultdict(list)
            orphans: list[dict] = []
            for trip in d.get("trips") or []:
                lid = trip.get("load_id")
                if _is_internal_trip_number(lid):
                    continue
                key = sheet_service._normalize_load_num(lid) if lid else ""
                row_num = None
                sn = None
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
                if row_num and sn:
                    grouped[(sn, row_num)].append(trip)
                else:
                    orphans.append(trip)

            for (sn, row_num), trip_list in grouped.items():
                lids = [t.get("load_id") for t in trip_list if t.get("load_id")]
                pdf_rate_sum = 0.0
                for t in trip_list:
                    try:
                        pdf_rate_sum += float(t.get("rate_gross") or 0)
                    except (TypeError, ValueError):
                        pass
                fields = sheet_service.get_settlement_compare_fields(row_num, sn, company=company)
                if not fields:
                    load_bad += 1
                    load_rows.append(
                        {
                            "Driver": driver_name or "-",
                            "PDF Load IDs": " + ".join(lids) if lids else "-",
                            "PDF Rate jami": pdf_rate_sum,
                            "Sheet": sn,
                            "Row": row_num,
                            "Sheet Load ID": "-",
                            "Sheet Rate": "-",
                            "Natija": "MOS KELMADI",
                            "Sabab": "Sheet qatori o'qilmadi",
                        }
                    )
                    continue
                sheet_load = fields.get("load_number")
                sheet_rate = fields.get("rate")
                sheet_driver = (fields.get("driver") or "").strip()
                # Driver bo'yicha topilmasa ham load id bo'yicha qator topilgan holatlarda
                # ID+Rate mosligi asosiy mezon bo'lib qoladi.
                id_ok = _pdf_trip_ids_match_sheet_cell(sheet_service, lids, sheet_load)
                rate_ok = _money_eq(pdf_rate_sum, sheet_rate)
                drv_ok = _drivers_match(driver_name, sheet_driver) if driver_name else True
                # Fallback: agar driver nomi mos kelmasa ham load id + rate aniq mos bo'lsa
                # bu holatni "fallback bo'yicha mos" deb qabul qilamiz.
                fallback_by_load = id_ok and rate_ok and not drv_ok
                if id_ok and rate_ok and (drv_ok or fallback_by_load):
                    load_ok += 1
                    if fallback_by_load:
                        natija, sabab = "MOS KELDI", "Driver nomi mos emas, load id bo'yicha fallback mos keldi"
                    else:
                        natija, sabab = "MOS KELDI", "-"
                else:
                    load_bad += 1
                    bits = []
                    if not id_ok:
                        bits.append("Load ID mos emas")
                    if not rate_ok:
                        bits.append(f"Rate mos emas (PDF {pdf_rate_sum} vs Sheet {sheet_rate})")
                    if not drv_ok:
                        bits.append("Driver mos emas")
                    natija, sabab = "MOS KELMADI", ", ".join(bits)
                load_rows.append(
                    {
                        "Driver": driver_name or "-",
                        "PDF Load IDs": " + ".join(lids) if lids else "-",
                        "PDF Rate jami": pdf_rate_sum,
                        "Sheet": sn,
                        "Row": row_num,
                        "Sheet Load ID": sheet_load,
                        "Sheet Rate": sheet_rate,
                        "Natija": natija,
                        "Sabab": sabab,
                    }
                )

            for trip in orphans:
                load_miss += 1
                load_rows.append(
                    {
                        "Driver": driver_name or "-",
                        "PDF Load IDs": trip.get("load_id") or "-",
                        "PDF Rate jami": trip.get("rate_gross"),
                        "Sheet": "-",
                        "Row": "-",
                        "Sheet Load ID": "-",
                        "Sheet Rate": "-",
                        "Natija": "MOS KELMADI",
                        "Sabab": "Load boardda topilmadi",
                    }
                )

        expenses_rows = []
        exp_sheet_names = sheet_service.get_expenses_all_sheet_names(company)
        candidate_exp_sheets = [
            _find_sheet_by_alias(exp_sheet_names, "Owner Operators"),
            _find_sheet_by_alias(exp_sheet_names, "Company Drivers"),
            _find_sheet_by_alias(exp_sheet_names, "TERMINATED"),
        ]
        candidate_exp_sheets = [x for x in candidate_exp_sheets if x]
        year_now = pd.Timestamp.now().year
        def _to_amount(v):
            if v is None:
                return None
            s = str(v).strip()
            if not s or s == "-":
                return None
            s = s.replace("$", "").replace(",", "").strip()
            m = re.search(r"-?\d+(?:\.\d+)?", s)
            if not m:
                return None
            try:
                return float(m.group(0))
            except ValueError:
                return None

        # Har bir expenses sheet uchun metadata ni oldindan yig'ib qo'yamiz.
        exp_contexts = []
        for sh_name in candidate_exp_sheets:
            ws = sheet_service.get_expenses_board(sh_name, company)
            if not ws:
                continue
            segments = _extract_sheet_segments(ws, year_now)
            if not segments:
                continue
            top_grid = ws.get("A1:Z10")
            fuel_cols, toll_cols = [], []
            for r in range(min(10, len(top_grid))):
                row_data = top_grid[r] if r < len(top_grid) else []
                for c in range(min(26, len(row_data))):
                    txt = str(row_data[c] or "").strip().lower()
                    if not txt:
                        continue
                    if "fuel" in txt and ("exp" in txt or "after" in txt or "amount" in txt):
                        fuel_cols.append(c + 1)
                    if "toll" in txt and ("exp" in txt or "amount" in txt):
                        toll_cols.append(c + 1)
            if not fuel_cols:
                fuel_cols = [5]
            if not toll_cols:
                toll_cols = [7]

            seg_match = segments[0]
            wp_start = parsed.get("work_period_start")
            for seg in segments:
                if wp_start and seg[0] <= wp_start <= seg[1]:
                    seg_match = seg
                    break
            seg_idx = segments.index(seg_match)
            seg_start_col = seg_match[2]
            seg_end_col = segments[seg_idx + 1][2] - 1 if seg_idx + 1 < len(segments) else 26
            fuel_col = next((fc for fc in fuel_cols if seg_start_col <= fc <= seg_end_col), fuel_cols[0])
            toll_col = next((tc for tc in toll_cols if seg_start_col <= tc <= seg_end_col), toll_cols[0])

            name_vals = ws.col_values(2)
            resolved_names = []
            cur = ""
            for raw in name_vals:
                s = str(raw or "").strip()
                if s and len(s) >= 3 and not re.match(r"^[\d\s$.,\-–—%/]+$", s):
                    cur = s
                resolved_names.append(cur)
            exp_contexts.append(
                {
                    "sheet_name": sh_name,
                    "ws": ws,
                    "fuel_col": fuel_col,
                    "toll_col": toll_col,
                    "resolved_names": resolved_names,
                }
            )

        # Muhim: har bir driver uchun faqat 1 ta eng mos row topiladi (ko'payib ketmasin).
        for d in parsed.get("drivers") or []:
            driver_name = str(d.get("driver_name") or "").strip()
            if not driver_name:
                continue
            fuel_total = _to_amount(d.get("fuel_total_pay_amount"))
            toll_total = _to_amount(d.get("toll_total_pay_amount"))
            if fuel_total is None and toll_total is None:
                continue

            best = None
            for ctx in exp_contexts:
                ws = ctx["ws"]
                resolved_names = ctx["resolved_names"]
                for i, drv in enumerate(resolved_names, start=1):
                    if i < 4:
                        continue
                    if not drv or not _drivers_match(driver_name, drv):
                        continue
                    sheet_fuel = _to_amount(ws.cell(i, ctx["fuel_col"]).value)
                    sheet_toll = _to_amount(ws.cell(i, ctx["toll_col"]).value)
                    score = 0
                    if fuel_total is not None and sheet_fuel is not None and _money_eq(fuel_total, sheet_fuel):
                        score += 1
                    if toll_total is not None and sheet_toll is not None and _money_eq(toll_total, sheet_toll):
                        score += 1
                    cand = {
                        "sheet_name": ctx["sheet_name"],
                        "row": i,
                        "sheet_fuel": sheet_fuel,
                        "sheet_toll": sheet_toll,
                        "score": score,
                    }
                    if best is None or cand["score"] > best["score"]:
                        best = cand

            if fuel_total is not None:
                if best:
                    ok = best["sheet_fuel"] is not None and _money_eq(fuel_total, best["sheet_fuel"])
                    expenses_rows.append(
                        {
                            "Driver": driver_name,
                            "Tekshiruv": "Fuel Total",
                            "Sheet": best["sheet_name"],
                            "Row": best["row"],
                            "PDF Amount": fuel_total,
                            "Sheet Amount": best["sheet_fuel"] if best["sheet_fuel"] is not None else "-",
                            "Natija": "MOS KELDI" if ok else "MOS KELMADI",
                            "Sabab": "-" if ok else "Fuel total mos emas",
                        }
                    )
                else:
                    expenses_rows.append(
                        {
                            "Driver": driver_name,
                            "Tekshiruv": "Fuel Total",
                            "Sheet": "-",
                            "Row": "-",
                            "PDF Amount": fuel_total,
                            "Sheet Amount": "-",
                            "Natija": "MOS KELMADI",
                            "Sabab": "Driver topilmadi",
                        }
                    )

            if toll_total is not None:
                if best:
                    ok = best["sheet_toll"] is not None and _money_eq(toll_total, best["sheet_toll"])
                    expenses_rows.append(
                        {
                            "Driver": driver_name,
                            "Tekshiruv": "Toll Total",
                            "Sheet": best["sheet_name"],
                            "Row": best["row"],
                            "PDF Amount": toll_total,
                            "Sheet Amount": best["sheet_toll"] if best["sheet_toll"] is not None else "-",
                            "Natija": "MOS KELDI" if ok else "MOS KELMADI",
                            "Sabab": "-" if ok else "Toll total mos emas",
                        }
                    )
                else:
                    expenses_rows.append(
                        {
                            "Driver": driver_name,
                            "Tekshiruv": "Toll Total",
                            "Sheet": "-",
                            "Row": "-",
                            "PDF Amount": toll_total,
                            "Sheet Amount": "-",
                            "Natija": "MOS KELMADI",
                            "Sabab": "Driver topilmadi",
                        }
                    )

        load_df = pd.DataFrame(load_rows or [])
        exp_df = pd.DataFrame(expenses_rows or [])
        base = re.sub(r'[<>:"/\\|?*]', "_", document.file_name or "report").strip() or "report"
        report_name = f"Contractor_check_{base}.xlsx"
        if not report_name.lower().endswith(".xlsx"):
            report_name += ".xlsx"

        with pd.ExcelWriter(report_name, engine="openpyxl") as writer:
            load_df.to_excel(writer, sheet_name="Load check", index=False)
            exp_df.to_excel(writer, sheet_name="Fuel Toll check", index=False)
            green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
            for ws_name in ("Load check", "Fuel Toll check"):
                wsx = writer.book[ws_name]
                natija_col = None
                for c in range(1, wsx.max_column + 1):
                    if str(wsx.cell(row=1, column=c).value or "").strip() == "Natija":
                        natija_col = c
                        break
                if natija_col:
                    for r in range(2, wsx.max_row + 1):
                        if str(wsx.cell(row=r, column=natija_col).value or "").strip().upper() == "MOS KELDI":
                            for c in range(1, wsx.max_column + 1):
                                wsx.cell(row=r, column=c).fill = green_fill

        warn_text = "\n".join(f"⚠️ {w}" for w in warnings) if warnings else ""
        if warn_text:
            await message.answer(warn_text)
        exp_ok = sum(1 for r in expenses_rows if r.get("Natija") == "MOS KELDI")
        exp_bad = sum(1 for r in expenses_rows if r.get("Natija") != "MOS KELDI")
        await message.answer(
            f"🏁 Contractor tekshiruv tugadi — <b>{company}</b>.\n"
            f"📦 Load: ✅ {load_ok} | ❌ {load_bad} | ❓ {load_miss}\n"
            f"⛽🛣️ Fuel/Toll: ✅ {exp_ok} | ❌ {exp_bad}",
            parse_mode="HTML",
        )
        await message.answer_document(types.FSInputFile(report_name))
        os.remove(report_name)
        await state.set_state(BotStates.Statement)
    except Exception as e:
        err = str(e)
        if "429" in err or "Quota" in err or "quota" in err:
            await message.answer("⚠️ Google Sheets daqiqalik limiti. 1–3 daqiqa kutib qayta yuboring.")
        else:
            await message.answer(f"Xatolik: {e}")
    finally:
        await asyncio.sleep(1.0)
        CONTRACTOR_PROCESS_LOCK.release()

