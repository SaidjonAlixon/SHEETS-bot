    def get_load_details(self, row, sheet_name):
        """
        Qator raqami bo'yicha kerakli ma'lumotlarni o'qish (Solishtirish uchun).
        Qaytaradi: {
            'invoiced': float,
            'broker_paid': float,
            'status': str
        }
        """
        sheet = self.get_load_board(sheet_name)
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
