import json
import os
import gspread
from gspread.exceptions import APIError as GspreadAPIError
from gspread.cell import Cell
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, date
import config
import pandas as pd
import time
import re

# Lazy singleton
_sheet_service_instance = None
_sheet_names_cache: dict[tuple, list] = {}
_sheet_names_cache_time: dict[tuple, float] = {}
CACHE_SEC = 90  # List nomlari 90 sekund cache

def get_sheet_service():
    """Lazy init - faqat kerak bo'lganda yaratiladi. 429 da retry."""
    global _sheet_service_instance
    if _sheet_service_instance is not None:
        return _sheet_service_instance
    _sheet_service_instance = GoogleSheetService()
    return _sheet_service_instance

class GoogleSheetService:
    def __init__(self):
        self.scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds_val = (config.GOOGLE_SHEETS_CREDENTIALS or "").strip()
        if creds_val.startswith("{") and os.path.isfile(creds_val) is False:
            # Railway: JSON string env dan
            creds_dict = json.loads(creds_val)
            self.creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, self.scope)
        else:
            # Lokal: fayl yo'li
            self.creds = ServiceAccountCredentials.from_json_keyfile_name(creds_val, self.scope)
        self.client = gspread.authorize(self.creds)
        self._load_spreadsheets: dict[str, object] = {}
        self._expenses_spreadsheets: dict[str, object] = {}

    def _get_company_or_default(self, company):
        return company if company and company in config.COMPANY_SHEET_KEYS else config.COMPANY_NAMES[0]

    def _get_load_spreadsheet(self, company=None):
        company = self._get_company_or_default(company)
        if company not in self._load_spreadsheets:
            keys = config.COMPANY_SHEET_KEYS.get(company, {})
            load_key = keys.get("load_key") or config.GOOGLE_SHEET_KEY
            for attempt in range(4):
                try:
                    self._load_spreadsheets[company] = self.client.open_by_key(load_key)
                    break
                except GspreadAPIError as e:
                    if "429" in str(e) and attempt < 3:
                        time.sleep(30 * (attempt + 1))
                    else:
                        raise
        return self._load_spreadsheets.get(company)

    def _get_expenses_spreadsheet(self, company=None):
        company = self._get_company_or_default(company)
        if company not in self._expenses_spreadsheets:
            keys = config.COMPANY_SHEET_KEYS.get(company, {})
            exp_key = keys.get("expenses_key") or config.GOOGLE_EXPENSES_SHEET_KEY
            if not exp_key:
                raise ValueError("GOOGLE_EXPENSES_SHEET_KEY .env da yo'q")
            for attempt in range(4):
                try:
                    self._expenses_spreadsheets[company] = self.client.open_by_key(exp_key)
                    break
                except GspreadAPIError as e:
                    if "429" in str(e) and attempt < 3:
                        time.sleep(30 * (attempt + 1))
                    else:
                        raise
        return self._expenses_spreadsheets.get(company)

    def _retry_on_429(self, func, *args, max_retries=4, **kwargs):
        """429 da avtomatik retry."""
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except GspreadAPIError as e:
                if "429" in str(e) and attempt < max_retries - 1:
                    wait = 20 + attempt * 25
                    time.sleep(wait)
                    continue
                raise

    def get_load_board(self, sheet_name=None, company=None):
        spreadsheet = self._get_load_spreadsheet(company)
        if not spreadsheet:
            return None
        def _get():
            if sheet_name:
                return spreadsheet.worksheet(sheet_name)
            return spreadsheet.sheet1
        try:
            return self._retry_on_429(_get)
        except gspread.WorksheetNotFound:
            return None

    def get_expenses_board(self, sheet_name=None, company=None):
        spreadsheet = self._get_expenses_spreadsheet(company)
        if not spreadsheet:
            return None
        def _get():
            if sheet_name:
                return spreadsheet.worksheet(sheet_name)
            return spreadsheet.sheet1
        try:
            return self._retry_on_429(_get)
        except gspread.WorksheetNotFound:
            return None

    def get_all_sheet_names(self, company=None):
        company = self._get_company_or_default(company)
        cache_key = ("load", company)
        now = time.time()
        if cache_key in _sheet_names_cache and (now - _sheet_names_cache_time.get(cache_key, 0)) < CACHE_SEC:
            return _sheet_names_cache[cache_key]
        spreadsheet = self._get_load_spreadsheet(company)
        def _get():
            return [ws.title for ws in spreadsheet.worksheets()]
        names = self._retry_on_429(_get)
        _sheet_names_cache[cache_key] = names
        _sheet_names_cache_time[cache_key] = now
        return names

    def get_expenses_all_sheet_names(self, company=None):
        company = self._get_company_or_default(company)
        spreadsheet = self._get_expenses_spreadsheet(company)
        return [ws.title for ws in spreadsheet.worksheets()]

    def get_sheet_by_date(self, date_obj, sheet_names=None, company=None):
        """
        Sana bo'yicha (datetime.date) mos keluvchi sheet nomini qaytaradi.
        Sheet format: "MM.DD-MM.DD" - qaysi kundan qaysi kungacha.
        """
        if not date_obj:
            return None
        if sheet_names is None:
            sheet_names = self.get_all_sheet_names(company)
        year = getattr(date_obj, 'year', None) or datetime.now().year

        # Sheet nomida prefix bo'lishi mumkin:
        # "2026 February/March 02.23-03.01" kabi.
        range_re = re.compile(r'(\d{1,2}\.\d{1,2})\s*-\s*(\d{1,2}\.\d{1,2})')

        for name in sheet_names:
            try:
                m = range_re.search(name)
                if not m:
                    continue
                start_str, end_str = m.group(1), m.group(2)
                start_month, start_day = map(int, start_str.split('.'))
                end_month, end_day = map(int, end_str.split('.'))

                start_date = date(year, start_month, start_day)
                # Agar period kesishsa (masalan 12.28-01.05) end_date keyingi yil bo'ladi.
                end_year = year
                if (end_month, end_day) < (start_month, start_day):
                    end_year = year + 1
                end_date = date(end_year, end_month, end_day)

                if start_date <= date_obj <= end_date:
                    return name
            except Exception:
                continue
        return None

    def _normalize_load_num(self, val):
        """
        LOAD#/CARD taqqoslash uchun.
        Muhim: uzun raqamlar (masalan card id) floatga aylantirilsa aniqlik yo'qoladi.
        Shuning uchun float ishlatmasdan stringga asoslangan normalizatsiya qilamiz.
        """
        if val is None:
            return ""
        if isinstance(val, float) and val != val:  # NaN
            return ""
        s = str(val).strip()
        if not s or s.lower() == "nan":
            return ""

        # Ilmiy yozuv bo'lishi mumkin: 7.083e+18 -> 7083... (int)
        if "e" in s.lower():
            try:
                from decimal import Decimal
                return str(int(Decimal(s)))
            except Exception:
                pass

        # Agar oxiri .0 bo'lsa (masalan '7083...0' yoki '123.0')
        if s.endswith(".0"):
            return s[:-2]

        # Agar decimal bor bo'lsa, fractional qismi hammasi nol bo'lsa int qiling
        if "." in s:
            left, right = s.split(".", 1)
            if right and set(right) == {"0"}:
                return left
            # boshqa holatlarda aynan stringni qaytaramiz
            return s

        return s

    def find_load_row(self, load_number, sheet_name, load_col=None, company=None):
        """
        LOAD # ustuni bo'yicha Load Numberni qidiradi.
        Odatiy: D=4, agar topilmasa E=5 sinash. 17-qatordan boshlab.
        """
        sheet = self.get_load_board(sheet_name, company)
        if not sheet:
            return None
        try:
            for col in (load_col,) if load_col else (4, 5, 7):  # D, E, G - LOAD #
                if col is None:
                    continue
                load_numbers = sheet.col_values(col)
                start_index = 16
                target = self._normalize_load_num(load_number)
                for i in range(start_index, len(load_numbers)):
                    if i < len(load_numbers) and self._normalize_load_num(load_numbers[i]) == target:
                        return i + 1
            return None
        except Exception as e:
            print(f"Error finding load: {e}")
            return None

    def get_date_sheet_names(self, company=None):
        """MM.DD-MM.DD formatidagi sheet nomlari. Bo'sh bo'lsa - barcha sheetlar."""
        result = []
        range_re = re.compile(r'(\d{1,2}\.\d{1,2})\s*-\s*(\d{1,2}\.\d{1,2})')
        for name in self.get_all_sheet_names(company):
            if range_re.search(name):
                result.append(name)
        # Agar date format bo'lmasa (masalan "Drivers & Dispatch") - barcha sheetlardan qidirish
        if not result:
            result = [n for n in self.get_all_sheet_names(company) if n.lower() != 'dashboard']
        return result

    def get_last_n_week_sheets(self, n=10, company=None):
        """Eng yangi n ta hafta (sana oralig'i bo'lgan) sheetlar. Oxirgi 10 hafta."""
        names = self.get_date_sheet_names(company)
        if not names:
            return []
        range_re = re.compile(r'(\d{1,2}\.\d{1,2})\s*-\s*(\d{1,2}\.\d{1,2})')
        year_re = re.compile(r'(\d{4})')
        now = datetime.now()
        year = now.year

        def _sort_key(name):
            m = range_re.search(name)
            if not m:
                return (0, 0, 0)
            start_str, end_str = m.group(1), m.group(2)
            y_m = year_re.search(name)
            y = int(y_m.group(1)) if y_m else year
            try:
                end_m, end_d = map(int, end_str.split('.'))
                if not y_m and end_m > now.month:
                    y = year - 1
                return (y, end_m, end_d)
            except (ValueError, TypeError):
                return (0, 0, 0)

        sorted_names = sorted(names, key=_sort_key, reverse=True)
        return sorted_names[:n]

    def update_factoring_across_sheets(self, sheet_names, parsed_data, load_cols=(4, 5, 7), start_row=17, company=None):
        """
        Bir nechta sheetda Load # ni qidirib, topilganiga summani yozadi.
        sheet_names: qidiriladigan sheetlar (eng yangi 10 hafta).
        Qaytaradi: (updated, skipped, not_found, results).
        """
        if not sheet_names or not parsed_data:
            return (0, 0, len(parsed_data), [])

        load_to_sheet_row = {}  # normalized_load -> (sheet_name, row_num, inv_cur_value)
        for sn in sheet_names:
            sheet = self.get_load_board(sn, company)
            if not sheet:
                continue
            try:
                all_rows = self._retry_on_429(sheet.get_all_values)
            except Exception:
                continue
            for i in range(start_row - 1, len(all_rows)):
                row = all_rows[i] if i < len(all_rows) else []
                for col in load_cols:
                    val = row[col - 1] if len(row) >= col else ""
                    norm = self._normalize_load_num(val)
                    if norm and norm not in load_to_sheet_row:
                        inv_cur = row[15] if len(row) > 15 else ""  # P ustuni = 16, index 15
                        load_to_sheet_row[norm] = (sn, i + 1, inv_cur)
                        break

        results = []
        cells_by_sheet = {}
        updated = 0
        skipped = 0
        not_found = 0

        for item in parsed_data:
            load_num = item.get("load_number", "") or ""
            amount = item.get("amount") or 0
            if not str(load_num).strip():
                results.append({"Load/PO #": load_num, "Invoice Amount": amount, "Sheet": "-", "Status": "EMPTY LOAD #"})
                continue

            target = self._normalize_load_num(load_num)
            found = load_to_sheet_row.get(target)
            if not found:
                not_found += 1
                results.append({"Load/PO #": load_num, "Invoice Amount": amount, "Sheet": "-", "Status": "LOAD NOT FOUND"})
                continue

            sheet_name, row_num, inv_cur = found
            if self._is_empty_or_zero(inv_cur):
                if sheet_name not in cells_by_sheet:
                    cells_by_sheet[sheet_name] = []
                cells_by_sheet[sheet_name].append(Cell(row=row_num, col=16, value=amount))
                cells_by_sheet[sheet_name].append(Cell(row=row_num, col=15, value="Invoiced"))
                updated += 1
                results.append({"Load/PO #": load_num, "Invoice Amount": amount, "Sheet": sheet_name, "Status": "UPDATED"})
            else:
                skipped += 1
                results.append({"Load/PO #": load_num, "Invoice Amount": amount, "Sheet": sheet_name, "Status": "SKIPPED"})

        for sn, cells in cells_by_sheet.items():
            ws = self.get_load_board(sn, company)
            if ws and cells:
                self._retry_on_429(ws.update_cells, cells)

        return (updated, skipped, not_found, results)

    def update_broker_payment_across_sheets(self, sheet_names, parsed_data, load_cols=(4, 5, 7), start_row=17, company=None):
        """
        Oxirgi 10 hafta sheetlarida Load # ni qidirib, Funded Amount ni R (BROKER PAID) ga yozadi.
        parsed_data: [{'load_number': '...', 'amount': summa}, ...] — bitta load uchun bitta, umumiy summa.
        """
        if not sheet_names or not parsed_data:
            return (0, 0, len(parsed_data) if parsed_data else 0, [])

        load_to_sheet_row = {}
        for sn in sheet_names:
            sheet = self.get_load_board(sn, company)
            if not sheet:
                continue
            try:
                all_rows = self._retry_on_429(sheet.get_all_values)
            except Exception:
                continue
            for i in range(start_row - 1, len(all_rows)):
                row = all_rows[i] if i < len(all_rows) else []
                for col in load_cols:
                    val = row[col - 1] if len(row) >= col else ""
                    norm = self._normalize_load_num(val)
                    if norm and norm not in load_to_sheet_row:
                        paid_cur = row[17] if len(row) > 17 else ""
                        load_to_sheet_row[norm] = (sn, i + 1, paid_cur)
                        break

        results = []
        cells_by_sheet = {}
        updated = 0
        skipped = 0
        not_found = 0

        for item in parsed_data:
            load_num = item.get("load_number", "") or ""
            amount = item.get("amount") or 0
            if not str(load_num).strip():
                results.append({"Load #": load_num, "Check Amount": amount, "Sheet": "-", "Status": "EMPTY LOAD #"})
                continue

            target = self._normalize_load_num(load_num)
            found = load_to_sheet_row.get(target)
            if not found:
                not_found += 1
                results.append({"Load #": load_num, "Check Amount": amount, "Sheet": "-", "Status": "NOT FOUND"})
                continue

            sheet_name, row_num, paid_cur = found
            if self._is_empty_or_zero(paid_cur):
                if sheet_name not in cells_by_sheet:
                    cells_by_sheet[sheet_name] = []
                cells_by_sheet[sheet_name].append(Cell(row=row_num, col=18, value=amount))
                cells_by_sheet[sheet_name].append(Cell(row=row_num, col=15, value="Broker paid"))
                updated += 1
                results.append({"Load #": load_num, "Check Amount": amount, "Sheet": sheet_name, "Status": "FOUND"})
            else:
                skipped += 1
                results.append({"Load #": load_num, "Check Amount": amount, "Sheet": sheet_name, "Status": "ALREADY FILLED"})

        for sn, cells in cells_by_sheet.items():
            ws = self.get_load_board(sn, company)
            if ws and cells:
                self._retry_on_429(ws.update_cells, cells)

        return (updated, skipped, not_found, results)

    async def find_load_in_any_sheet_async(self, load_number, sheet_names=None, company=None):
        """Barcha date sheetlarda Load # ni qidiradi. Topilmasa - (None, None)."""
        import asyncio
        if sheet_names is None:
            sheet_names = self.get_date_sheet_names(company)
        for i, sn in enumerate(sheet_names):
            if i > 0:
                await asyncio.sleep(1.5)
            row_num = self.find_load_row(load_number, sn, company=company)
            if row_num:
                return (row_num, sn)
        return (None, None)
            
    def update_cell(self, row, col, value, sheet_name="LOAD BOARD", company=None):
        sheet = self.get_load_board(sheet_name, company)
        if sheet:
            sheet.update_cell(row, col, value)

    def update_factoring(self, row, invoiced_amount, sheet_name, company=None):
        """
        P (16) ustuniga INVOICED AMOUNT yozish
        O (15) ustuniga STATUS = 'Invoiced' yozish
        """
        if invoiced_amount is None:
            return False
        sheet = self.get_load_board(sheet_name, company)
        if not sheet: return False
        
        current_amount = sheet.cell(row, 16).value
        if self._is_empty_or_zero(current_amount):
            sheet.update_cell(row, 16, invoiced_amount)
            sheet.update_cell(row, 15, "Invoiced")
            return True
        return False

    def update_factoring_batch(self, sheet_name, parsed_data, load_cols=(4, 5, 7), start_row=17, company=None):
        """
        Bir marta o'qib, batch yozish — Fuel/Toll kabi tez.
        parsed_data: [{"load_number": "...", "amount": ...}, ...]
        Qaytaradi: (updated, skipped, not_found, results) — results har biri uchun status.
        """
        sheet = self.get_load_board(sheet_name, company)
        if not sheet:
            return (0, 0, len(parsed_data), [])

        load_to_row = {}
        for col in load_cols:
            vals = sheet.col_values(col)
            for i in range(start_row - 1, len(vals)):
                norm = self._normalize_load_num(vals[i])
                if norm and norm not in load_to_row:
                    load_to_row[norm] = i + 1

        inv_vals = sheet.col_values(16)
        status_vals = sheet.col_values(15)

        cells = []
        updated = 0
        skipped = 0
        not_found = 0
        results = []

        for item in parsed_data:
            load_num = item.get("load_number", "") or ""
            amount = item.get("amount") or 0
            if not str(load_num).strip():
                results.append({"Load/PO #": load_num, "Invoice Amount": amount, "Status": "EMPTY LOAD #"})
                continue

            target = self._normalize_load_num(load_num)
            row_num = load_to_row.get(target)
            if not row_num:
                not_found += 1
                results.append({"Load/PO #": load_num, "Invoice Amount": amount, "Status": "LOAD NOT FOUND"})
                continue

            inv_cur = inv_vals[row_num - 1] if row_num - 1 < len(inv_vals) else ""
            if self._is_empty_or_zero(inv_cur):
                cells.append(Cell(row=row_num, col=16, value=amount))
                cells.append(Cell(row=row_num, col=15, value="Invoiced"))  # STATUS dropdown: Booked, Invoiced, Broker paid, ...
                updated += 1
                results.append({"Load/PO #": load_num, "Invoice Amount": amount, "Status": "UPDATED"})
            else:
                skipped += 1
                results.append({"Load/PO #": load_num, "Invoice Amount": amount, "Status": "SKIPPED (allaqachon to'ldirilgan)"})

        if cells:
            sheet.update_cells(cells)
        return (updated, skipped, not_found, results)

    def _is_empty_or_zero(self, val):
        """Bo'sh yoki $0.00 - yangilash mumkin."""
        if val is None or val == "" or val == "-" or val == "—":
            return True
        s = str(val).strip().replace("$", "").replace(",", "")
        try:
            return float(s) == 0
        except ValueError:
            return False

    def update_broker_payment(self, row, paid_amount, sheet_name, company=None):
        """
        R (18) ustuniga BROKER PAID, O (15) ustuniga STATUS = 'Broker paid' yozish.
        Xls dagi Invoice Amount qiymati shu yerga yoziladi.
        """
        if paid_amount is None:
            return False
        sheet = self.get_load_board(sheet_name, company)
        if not sheet: return False
        
        current_paid = sheet.cell(row, 18).value  # R ustuni - BROKER PAID
        if self._is_empty_or_zero(current_paid):
            sheet.update_cell(row, 18, paid_amount)
            sheet.update_cell(row, 15, "Broker paid")  # STATUS dropdown dan
            return True
        return False

    def update_broker_payment_batch(self, sheet_name, parsed_data, load_cols=(4, 5, 7), start_row=17, company=None):
        """
        Bir marta o'qib, batch yozish — Factoring/Fuel/Toll kabi tez.
        get_all_values() bilan 1 API chaqiruv — 4 ta col_values o'rniga.
        """
        sheet = self.get_load_board(sheet_name, company)
        if not sheet:
            return (0, 0, len(parsed_data), [])

        all_rows = self._retry_on_429(sheet.get_all_values)
        load_to_row = {}
        paid_vals = []
        for i in range(start_row - 1, len(all_rows)):
            row = all_rows[i] if i < len(all_rows) else []
            for col in load_cols:
                val = row[col - 1] if len(row) >= col else ""
                norm = self._normalize_load_num(val)
                if norm and norm not in load_to_row:
                    load_to_row[norm] = i + 1
                    break
            paid_val = row[17] if len(row) >= 18 else ""
            paid_vals.append(paid_val)

        cells = []
        updated = 0
        skipped = 0
        not_found = 0
        results = []

        for item in parsed_data:
            load_num = item.get("load_number", "") or ""
            broker_amount = item.get("invoice_amount") if item.get("invoice_amount") is not None else item.get("amount")
            if not str(load_num).strip():
                results.append({"Load #": load_num, "Invoice Amount": broker_amount, "Broker Amount": broker_amount, "Date": item.get("date"), "Status": "EMPTY LOAD #"})
                continue

            target = self._normalize_load_num(load_num)
            row_num = load_to_row.get(target)
            if not row_num:
                not_found += 1
                results.append({"Load #": load_num, "Invoice Amount": broker_amount, "Broker Amount": broker_amount, "Date": item.get("date"), "Status": "LOAD NOT FOUND"})
                continue

            paid_cur = paid_vals[row_num - start_row] if (row_num >= start_row and row_num - start_row < len(paid_vals)) else ""
            if self._is_empty_or_zero(paid_cur):
                cells.append(Cell(row=row_num, col=18, value=broker_amount))
                cells.append(Cell(row=row_num, col=15, value="Broker paid"))
                updated += 1
                results.append({"Load #": load_num, "Invoice Amount": broker_amount, "Broker Amount": broker_amount, "Date": item.get("date"), "Status": f"UPDATED ({sheet_name})"})
            else:
                skipped += 1
                results.append({"Load #": load_num, "Invoice Amount": broker_amount, "Broker Amount": broker_amount, "Date": item.get("date"), "Status": f"SKIPPED ({sheet_name})"})

        if cells:
            sheet.update_cells(cells)
        return (updated, skipped, not_found, results)

    def get_load_to_row_map(self, sheet_name, load_cols=(4, 5, 7), start_row=17, company=None):
        """Sheetdagi load_number -> row_num xaritasi. 1 API — get_all_values."""
        sheet = self.get_load_board(sheet_name, company)
        if not sheet:
            return {}
        all_rows = self._retry_on_429(sheet.get_all_values)
        load_to_row = {}
        for i in range(start_row - 1, len(all_rows)):
            row = all_rows[i] if i < len(all_rows) else []
            for col in load_cols:
                val = row[col - 1] if len(row) >= col else ""
                norm = self._normalize_load_num(val)
                if norm and norm not in load_to_row:
                    load_to_row[norm] = i + 1
                    break
        return load_to_row

    def add_fuel_expense(self, data):
        """
        FUEL_EXPENSES sheetiga yozish.
        Data: [Driver Name, Card Number, Amount, Week, Source File, Status]
        """
        sheet = self.get_expenses_board("FUEL_EXPENSES")
        if not sheet: 
            # Create if not exists logic could be here, but assume it exists
            return False
        
        sheet.append_row(data)
        return True

    def add_toll_expense(self, data):
        """
        TOLL_EXPENSES sheetiga yozish.
        Data: [Driver Name, Transponder, Amount, Week, Source, Status]
        """
        sheet = self.get_expenses_board("TOLL_EXPENSES")
        if not sheet: return False
        
        sheet.append_row(data)
        return True

    def find_card_row_in_expenses(self, card_number, sheet_name, card_col=3, start_row=4, company=None):
        """
        Expenses sheetida (ikkinchi spreadsheet), C4 kabi ko'rinishdagi card-larni topadi.
        card_col: 1-indexed ustun raqami (C=3).
        """
        sheet = self.get_expenses_board(sheet_name, company)
        if not sheet:
            return None
        try:
            cards = sheet.col_values(card_col)
            target = self._normalize_load_num(card_number)
            for i in range(start_row - 1, len(cards)):
                if self._normalize_load_num(cards[i]) == target:
                    return i + 1
            return None
        except Exception as e:
            print(f"find_card_row_in_expenses error: {e}")
            return None

    def update_fuel_toll_expenses(self, sheet_name, card_totals, fuel_col=5, discount_col=6, card_col=3, start_row=4, company=None):
        """
        card_totals: {card: (fuel_sum, discount_sum)}
        C (card) ustunidan qator raqamini topib:
          - fuel_col (masalan E) ga fuel_sum yozadi
          - discount_col (masalan F) ga discount_sum yozadi (Disc Amt, Toll Exp emas)

        Optimallashtirish: card/fuel/discount ustunlarini 1 marta o'qib, keyin local map bilan ishlaydi.
        """
        sheet = self.get_expenses_board(sheet_name, company)
        if not sheet:
            return (0, 0, 0, [])

        # 1 marta o'qish (API sonini keskin kamaytiradi)
        cards = sheet.col_values(card_col)
        fuel_vals = sheet.col_values(fuel_col)
        discount_vals = sheet.col_values(discount_col)

        # card_normalized -> row_num
        card_to_row = {}
        for i in range(start_row - 1, len(cards)):
            raw = cards[i]
            norm = self._normalize_load_num(raw)
            if not norm:
                continue
            if norm not in card_to_row:
                card_to_row[norm] = i + 1  # gspread 1-indexed qator

        updated = 0
        skipped = 0
        missing_cards = []
        cells = []

        for card, totals in card_totals.items():
            if not totals:
                continue
            fuel_sum, discount_sum = totals
            target = self._normalize_load_num(card)
            if not target:
                continue

            row = card_to_row.get(target)
            if not row:
                missing_cards.append(card)
                continue

            fuel_cur = fuel_vals[row - 1] if row - 1 < len(fuel_vals) else ""
            discount_cur = discount_vals[row - 1] if row - 1 < len(discount_vals) else ""

            # Faqat bo'sh/0 bo'lsa yozamiz
            if self._is_empty_or_zero(fuel_cur):
                cells.append(Cell(row=row, col=fuel_col, value=fuel_sum))
                updated += 1
            else:
                skipped += 1

            if self._is_empty_or_zero(discount_cur):
                cells.append(Cell(row=row, col=discount_col, value=discount_sum))
            else:
                skipped += 1

        if cells:
            sheet.update_cells(cells)
        return (updated, skipped, len(missing_cards), missing_cards)

    def update_toll_expenses(self, sheet_name, transponder_totals, toll_col, transponder_col=4, start_row=4, company=None):
        """
        transponder_totals: {pp_device_id: toll_sum}
        Transponder (D) ustunidan qator topib, toll_col ga toll_sum yozadi.
        """
        sheet = self.get_expenses_board(sheet_name, company)
        if not sheet:
            return (0, 0, 0, [])

        transponders = sheet.col_values(transponder_col)
        toll_vals = sheet.col_values(toll_col)

        trans_to_row = {}
        for i in range(start_row - 1, len(transponders)):
            raw = transponders[i]
            norm = self._normalize_load_num(raw)
            if not norm:
                continue
            if norm not in trans_to_row:
                trans_to_row[norm] = i + 1

        updated = 0
        skipped = 0
        missing = []
        cells = []

        for pp_id, toll_sum in transponder_totals.items():
            if (toll_sum or 0) == 0:
                continue
            target = self._normalize_load_num(pp_id)
            if not target:
                continue

            row = trans_to_row.get(target)
            if not row:
                missing.append(pp_id)
                continue

            toll_cur = toll_vals[row - 1] if row - 1 < len(toll_vals) else ""
            if self._is_empty_or_zero(toll_cur):
                cells.append(Cell(row=row, col=toll_col, value=toll_sum))
                updated += 1
            else:
                skipped += 1

        if cells:
            sheet.update_cells(cells)
        return (updated, skipped, len(missing), missing)

    def get_row_display(self, row, sheet_name, company=None):
        """Bir qator ma'lumotini ko'rsatish uchun. LOAD #, Driver, PU DATE, INVOICED, BROKER PAID, STATUS."""
        sheet = self.get_load_board(sheet_name, company)
        if not sheet:
            return None
        try:
            return {
                'load_number': sheet.cell(row, 4).value,
                'driver': sheet.cell(row, 2).value,
                'pu_date': sheet.cell(row, 5).value,
                'invoiced': sheet.cell(row, 16).value,
                'broker_paid': sheet.cell(row, 18).value,
                'status': sheet.cell(row, 15).value,
            }
        except Exception as e:
            print(f"get_row_display error: {e}")
            return None

    def get_recent_loads(self, sheet_name, limit=15, company=None):
        """Sheetdan oxirgi N ta yuk (LOAD # bo'lgan qatorlar). Data 17-qatordan boshlanadi."""
        sheet = self.get_load_board(sheet_name, company)
        if not sheet:
            return []
        try:
            load_col = 4
            load_vals = sheet.col_values(load_col)
            if len(load_vals) <= 16:
                return []
            rows = []
            for i in range(16, len(load_vals)):
                val = self._normalize_load_num(load_vals[i])
                if val and val.lower() != 'load #':
                    rows.append((i + 1, val))
            rows = rows[-limit:]
            return [self.get_row_display(r[0], sheet_name, company) for r in rows]
        except Exception as e:
            print(f"get_recent_loads error: {e}")
            return []

    def find_load_sync(self, load_number, sheet_names=None, company=None):
        """Barcha sheetlarda Load # qidirish. Qaytaradi (row_num, sheet_name) yoki (None, None)."""
        if sheet_names is None:
            sheet_names = self.get_date_sheet_names(company)
        for sn in sheet_names:
            row_num = self.find_load_row(load_number, sn, company=company)
            if row_num:
                return (row_num, sn)
        return (None, None)

    def get_sheet_summary(self, sheet_name, company=None):
        """List bo'yicha hisobot: yozuvlar soni, jami INVOICED, jami BROKER PAID."""
        sheet = self.get_load_board(sheet_name, company)
        if not sheet:
            return None
        try:
            count = 0
            total_inv = 0.0
            total_paid = 0.0
            load_vals = sheet.col_values(4)
            for i in range(16, len(load_vals)):
                if not self._normalize_load_num(load_vals[i]) or str(load_vals[i]).lower() == 'load #':
                    continue
                count += 1
                inv = sheet.cell(i + 1, 16).value
                paid = sheet.cell(i + 1, 18).value
                if inv:
                    try:
                        total_inv += float(str(inv).replace('$', '').replace(',', '').strip())
                    except ValueError:
                        pass
                if paid:
                    try:
                        total_paid += float(str(paid).replace('$', '').replace(',', '').strip())
                    except ValueError:
                        pass
            return {'count': count, 'total_invoiced': total_inv, 'total_broker_paid': total_paid}
        except Exception as e:
            print(f"get_sheet_summary error: {e}")
            return None

    def get_load_details(self, row, sheet_name, company=None):
        """
        Qator raqami bo'yicha kerakli ma'lumotlarni o'qish (Solishtirish uchun).
        Qaytaradi: {
            'invoiced': float,
            'broker_paid': float,
            'status': str
        }
        """
        sheet = self.get_load_board(sheet_name, company)
        if not sheet: return None
        
        try:
            # P=16 (Invoiced), R=18 (Broker Paid), O=15 (Status)
            invoiced = sheet.cell(row, 16).value
            broker_paid = sheet.cell(row, 18).value
            status = sheet.cell(row, 15).value
            
            # Helper function to clean amount strings (e.g. "$1,200.00" -> 1200.0)
            def clean_amount(val):
                if not val: return 0.0
                if isinstance(val, (int, float)): return float(val)
                val = str(val).replace('$', '').replace(',', '').strip()
                try:
                    return float(val)
                except:
                    return 0.0

            return {
                'invoiced': clean_amount(invoiced),
                'broker_paid': clean_amount(broker_paid),
                'status': str(status) if status else ""
            }
        except Exception as e:
            print(f"Error getting load details: {e}")
            return None

