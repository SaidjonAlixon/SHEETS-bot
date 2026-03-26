import re
import pandas as pd
import io

class ExcelParser:
    @staticmethod
    def parse_invoice(file_content):
        """
        Invoice faylini o'qish.
        Kutilayotgan ustunlar: 'Load #', 'Invoice Amount', 'Date' (ixtiyoriy, lekin sheetni topish uchun kerak)
        Qaytaradi: [{'load_number': '123', 'amount': 1000, 'date': datetime_obj}, ...]
        """
        try:
            # Faylni pandas bilan o'qish
            try:
                df = pd.read_excel(io.BytesIO(file_content))
            except:
                df = pd.read_csv(io.BytesIO(file_content))

            # Ustun nomlarini normallashtirish
            df.columns = df.columns.astype(str).str.lower().str.strip()
            
            # Kerakli ustunlarni qidirish
            load_col = None
            amount_col = None
            date_col = None
            
            for col in df.columns:
                if 'load' in col and ('#' in col or 'number' in col or 'no' in col or 'id' in col): 
                    load_col = col
                elif 'amount' in col or 'total' in col or 'rate' in col or 'invoice am' in col: 
                    amount_col = col
                elif 'date' in col or 'time' in col:
                    date_col = col
            
            if not load_col:
                return [] 
                
            results = []
            for _, row in df.iterrows():
                load_num = row[load_col]
                amount = row[amount_col] if amount_col else 0
                date_val = row[date_col] if date_col else None
                
                # LOAD # bo'lmasa - tashab o'tish
                if pd.isna(load_num) or not str(load_num).strip() or str(load_num).strip().lower() == 'nan':
                    continue
                load_str = str(load_num).strip()
                try:
                    amount_val = float(amount) if pd.notna(amount) else 0
                except (ValueError, TypeError):
                    m = re.search(r'-?\d+\.?\d*', str(amount or ''))
                    amount_val = float(m.group()) if m else 0

                parsed_date = None
                if pd.notna(date_val):
                    try:
                        parsed_date = pd.to_datetime(date_val).date()
                    except:
                        pass

                results.append({
                    'load_number': load_str,
                    'amount': amount_val,
                    'invoice_amount': amount_val,  # fallback: bir ustun bo'lsa
                    'date': parsed_date
                })
            return results
        except Exception as e:
            print(f"Excel parse error: {e}")
            return []

    @staticmethod
    def parse_factoring_report(file_content):
        """
        Factoring Payments fayli.
        Qo'llab-quvvatlanadi: Load/PO # (D), Load Number; faqat Funded Amount (H).
        Qaytaradi: [{'load_number': '...', 'amount': ...}, ...]
        """
        try:
            try:
                df = pd.read_excel(io.BytesIO(file_content))
            except Exception:
                df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8', encoding_errors='ignore')
            df.columns = [str(c).strip() for c in df.columns]
            load_col = None
            funded_col = None
            for i, col in enumerate(df.columns):
                col_lower = str(col).lower()
                # Load: Load/PO #, Load Number, Load # va hokazo
                if 'load' in col_lower and ('po' in col_lower or '#' in col_lower or 'number' in col_lower or i == 3):
                    load_col = col
                # Factoring uchun ustuvor: Funded Amount (H)
                elif 'funded amount' in col_lower or ('funded' in col_lower and 'amount' in col_lower):
                    funded_col = col
            if load_col is None and len(df.columns) > 1:
                load_col = df.columns[1]  # B - odatda Load Number
            if funded_col is None and len(df.columns) > 7:
                funded_col = df.columns[7]  # H - odatda Funded Amount
            if load_col is None or funded_col is None:
                return []
            results = []
            for _, row in df.iterrows():
                load_num = row.get(load_col)
                if pd.isna(load_num) or not str(load_num).strip() or str(load_num).strip().lower() == 'nan':
                    continue
                load_str = str(load_num).strip()
                inv_val = row.get(funded_col)
                amount_val = 0.0
                if pd.notna(inv_val):
                    s = str(inv_val).strip().replace(',', '.')
                    m = re.search(r'-?\d+\.?\d*', s)
                    if m:
                        amount_val = float(m.group())
                results.append({'load_number': load_str, 'amount': amount_val})
            return results
        except Exception as e:
            print(f"parse_factoring_report error: {e}")
            return []

    @staticmethod
    def parse_purchase_history_report(file_content):
        """
        Purchase History Details Report (sanalar yo'q).
        D: Load/PO #, F: Fee, H: Funded Amount.
        Bir xil Load ID bir necha marta bo'lsa — summani yig'adi (umumiy).
        Qaytaradi: [{'load_number': '...', 'amount': summa}, ...] — har load uchun 1 ta.
        amount = Funded Amount + Fee.
        """
        try:
            try:
                df = pd.read_excel(io.BytesIO(file_content))
            except Exception:
                df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8', encoding_errors='ignore')
            df.columns = [str(c).strip() for c in df.columns]
            load_col = None
            fee_col = None
            funded_col = None
            for i, col in enumerate(df.columns):
                col_lower = str(col).lower()
                if 'load' in col_lower and ('po' in col_lower or '#' in col_lower or 'number' in col_lower):
                    load_col = col
                elif col_lower == 'fee' or ('fee' in col_lower and 'funded' not in col_lower):
                    fee_col = col
                elif 'funded amount' in col_lower or ('funded' in col_lower and 'amount' in col_lower):
                    funded_col = col
            if load_col is None and len(df.columns) > 3:
                load_col = df.columns[3]
            if fee_col is None and len(df.columns) > 5:
                fee_col = df.columns[5]
            if funded_col is None and len(df.columns) > 7:
                funded_col = df.columns[7]
            if load_col is None or funded_col is None:
                return []
            aggregated = {}

            def _to_amount(v):
                if pd.isna(v):
                    return 0.0
                s = str(v).strip().replace(',', '.')
                m = re.search(r'-?\d+\.?\d*', s)
                return float(m.group()) if m else 0.0

            for _, row in df.iterrows():
                load_num = row.get(load_col)
                if pd.isna(load_num) or not str(load_num).strip() or str(load_num).strip().lower() == 'nan':
                    continue
                load_str = str(load_num).strip()
                funded_amount = _to_amount(row.get(funded_col))
                fee_amount = _to_amount(row.get(fee_col)) if fee_col else 0.0
                amount = funded_amount + fee_amount
                aggregated[load_str] = aggregated.get(load_str, 0) + amount
            return [{'load_number': k, 'amount': v} for k, v in aggregated.items()]
        except Exception as e:
            print(f"parse_purchase_history_report error: {e}")
            return []

    @staticmethod
    def parse_broker_payments_xls(file_content):
        """
        Broker Payments .xls faylini o'qish (1-rasm format).
        Asosiy format:
        - Load ID: D (Load #/Load/PO #) yoki B (Load Number)
        - Amount: faqat J (Check Amount)
        - Date: C (ixtiyoriy)
        """
        try:
            try:
                # Tezlik uchun faqat kerakli ustunlarni o'qiymiz: B,C,D va J
                df = pd.read_excel(io.BytesIO(file_content), usecols=[1, 2, 3, 9], dtype=str)
            except Exception:
                df = pd.read_csv(io.BytesIO(file_content), encoding='utf-8', encoding_errors='ignore')
            
            # Ustun nomlarini normallashtirish
            df.columns = [str(c).strip() for c in df.columns]
            
            # D/B, C, J/H - nomlar orqali yoki index orqali
            load_col = None
            date_col = None
            check_col = None
            
            for i, col in enumerate(df.columns):
                col_lower = str(col).lower()
                if 'load/po #' in col_lower or 'load #' in col_lower or 'load number' in col_lower or (i in (1, 3) and 'load' in col_lower):
                    load_col = col
                elif 'purchase date' in col_lower or 'payment date' in col_lower or (i in (2, 3) and 'date' in col_lower):
                    date_col = col
                elif 'check amount' in col_lower or (i == 9 and 'amount' in col_lower):
                    check_col = col
            
            # Agar nom topilmasa - index orqali (D=3, B=1, C=2, J=9, H=7)
            if load_col is None and len(df.columns) > 3:
                load_col = df.columns[3]
            if load_col is None and len(df.columns) > 1:
                load_col = df.columns[1]
            if date_col is None and len(df.columns) > 2:
                date_col = df.columns[2]
            if check_col is None and len(df.columns) > 9:
                check_col = df.columns[9]
            
            if load_col is None or check_col is None:
                return []
            
            results = []
            for _, row in df.iterrows():
                load_num = row.get(load_col)
                if pd.isna(load_num) or not str(load_num).strip() or str(load_num).strip().lower() == 'nan':
                    continue
                load_str = str(load_num).strip()
                
                date_val = row.get(date_col) if date_col else None
                parsed_date = None
                if pd.notna(date_val):
                    try:
                        parsed_date = pd.to_datetime(date_val).date()
                    except Exception:
                        pass
                
                inv_val = row.get(check_col)
                amount_val = 0.0
                if pd.notna(inv_val):
                    s = str(inv_val).strip()
                    m = re.search(r'-?\d+[.,]?\d*', s)
                    if m:
                        amount_val = float(m.group().replace(',', '.'))
                
                results.append({
                    'load_number': load_str,
                    'amount': amount_val,
                    'invoice_amount': amount_val,
                    'date': parsed_date
                })
            return results
        except Exception as e:
            print(f"parse_broker_payments_xls error: {e}")
            return []

    @staticmethod
    def parse_broker_report(file_content):
        """
        Broker Report faylini o'qish (Custom Logic).
        """
        try:
            try:
                df = pd.read_excel(io.BytesIO(file_content))
            except:
                df = pd.read_csv(io.BytesIO(file_content))

            # Normalize columns
            df.columns = df.columns.astype(str).str.lower().str.strip()
            
            # Identify columns
            load_col = None
            amount_col = None  # Check Amount -> BROKER PAID
            invoice_amount_col = None  # Invoice Amount -> INVOICED AMOUNT
            date_col = None
            purchase_date_col = None
            
            for col in df.columns:
                if 'load number' in col or 'load #' in col:
                    load_col = col
                elif 'check amount' in col:
                    amount_col = col
                elif 'invoice amount' in col or 'invoice am' in col:
                    invoice_amount_col = col
                elif 'purchase date' in col or 'pu date' in col:
                    purchase_date_col = col
                elif 'payment date' in col:
                    date_col = col

            if not invoice_amount_col:
                for col in df.columns:
                    if 'invoice' in col and 'amount' in col:
                        invoice_amount_col = col
                        break
            if not load_col:
                for col in df.columns:
                    if 'load' in col: load_col = col; break
            if purchase_date_col:
                date_col = purchase_date_col  # Sheet: 08.06-08.12 = yuk haftasi
            if not amount_col:
                for col in df.columns:
                    if 'amount' in col or 'total' in col: amount_col = col; break
            if not date_col:
                for col in df.columns:
                    if 'date' in col: date_col = col; break

            if not load_col:
                return []

            results = []
            for _, row in df.iterrows():
                load_num = row[load_col]
                amount = row[amount_col] if amount_col else 0
                inv_amt = row[invoice_amount_col] if invoice_amount_col else None
                date_val = row[date_col] if date_col else None
                
                if pd.isna(load_num) or not str(load_num).strip() or str(load_num).strip().lower() == 'nan':
                    continue
                load_str = str(load_num).strip()
                try:
                    amount_val = float(amount) if pd.notna(amount) else 0.0
                except (ValueError, TypeError):
                    m = re.search(r'-?\d+\.?\d*', str(amount or ''))
                    amount_val = float(m.group()) if m else 0.0
                try:
                    invoice_val = float(inv_amt) if pd.notna(inv_amt) else None
                except (ValueError, TypeError):
                    m = re.search(r'-?\d+\.?\d*', str(inv_amt or ''))
                    invoice_val = float(m.group()) if m else None
                
                parsed_date = None
                if pd.notna(date_val):
                    try:
                        parsed_date = pd.to_datetime(date_val).date()
                    except: pass
                
                results.append({
                    'load_number': load_str,
                    'amount': amount_val,
                    'invoice_amount': invoice_val,
                    'date': parsed_date
                })
            
            return results
        except Exception as e:
            print(f"Broker/Excel parse error: {e}")
            return []
