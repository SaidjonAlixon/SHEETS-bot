import os
from dotenv import load_dotenv

# .env faylini yuklash
load_dotenv()

# Bot Token
BOT_TOKEN = os.getenv("BOT_TOKEN")

# Adminlar ID ro'yxati (.env da vergul bilan: 123,456,789)
ADMINS = [a.strip() for a in os.getenv("ADMIN_IDS", "").split(",") if a.strip()]

# Google Sheets Config
GOOGLE_SHEETS_CREDENTIALS = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON")
GOOGLE_SHEET_KEY = os.getenv("GOOGLE_SHEET_KEY")
GOOGLE_EXPENSES_SHEET_KEY = os.getenv("GOOGLE_EXPENSES_SHEET_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1").strip()

# 5 kompaniya: har birida load (Factoring+Broker) va expenses (Fuel+Toll) sheetlar
COMPANY_NAMES = ("DELO", "MNK", "BUTATA", "AKA FS", "NYBC LLC")

def _get_company_keys():
    """Kompaniya -> {load_key, expenses_key} xaritasi. Yo'q bo'lsa global kalitdan foydalanadi."""
    default_load = GOOGLE_SHEET_KEY
    default_exp = GOOGLE_EXPENSES_SHEET_KEY
    mapping = {}
    for name in COMPANY_NAMES:
        slug = name.replace(" ", "_").replace(".", "")
        load_key = os.getenv(f"COMPANY_{slug}_LOAD_KEY") or default_load
        exp_key = os.getenv(f"COMPANY_{slug}_EXPENSES_KEY") or default_exp
        mapping[name] = {"load_key": load_key, "expenses_key": exp_key}
    return mapping

COMPANY_SHEET_KEYS = _get_company_keys()

# Load board: Company Driver settlement PDF dagi Rate (Gross) bilan solishtiriladigan ustun (1=A, 12=L, ...)
LOAD_BOARD_RATE_COL = int(os.getenv("LOAD_BOARD_RATE_COL", "11"))

# Database Config (DATABASE_URL yoki alohida parametrlar)
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL and all([os.getenv("DB_USER"), os.getenv("DB_PASS"), os.getenv("DB_NAME")]):
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"