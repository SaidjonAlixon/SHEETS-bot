# Railway ga deploy qilish

## 1. Railway hisob yarating
- [railway.app](https://railway.app) ga kiring
- GitHub bilan ulang

## 2. Yangi loyiha
- "New Project" → "Deploy from GitHub repo"
- LOad_tg_bot repozitoriyasini tanlang

## 3. PostgreSQL qo'shish
- Project ichida "New" → "Database" → "PostgreSQL"
- Railway avtomatik `DATABASE_URL` env qo'shadi

## 4. Environment Variables
Railway → Your Service → Variables:

| O'zgaruvchi | Qiymat |
|-------------|--------|
| BOT_TOKEN | Telegram Bot Token (@BotFather) |
| ADMIN_IDS | 123456789 (vergul bilan bir nechta) |
| DATABASE_URL | Avtomatik (PostgreSQL qo'shilganda) |
| GOOGLE_SHEETS_CREDENTIALS_JSON | service_account.json ichidagi to'liq JSON matn |
| GOOGLE_SHEET_KEY | Load sheet ID |
| GOOGLE_EXPENSES_SHEET_KEY | Expenses sheet ID |
| COMPANY_*_LOAD_KEY | Har bir kompaniya uchun (ixtiyoriy) |
| COMPANY_*_EXPENSES_KEY | Har bir kompaniya uchun (ixtiyoriy) |

### GOOGLE_SHEETS_CREDENTIALS_JSON (Railway)
Lokal `service_account.json` faylini oching, barcha matnni nusxalang va Railway Variables ga yoping. Bitta qator bo'lishi kerak.

## 5. Deploy
- Git push qiling: `git push origin main`
- Railway avtomatik build va deploy qiladi
- Procfile: `worker: python main.py`

## 6. Tekshirish
- Logs bo'limida "Bot ishga tushmoqda..." ko'rinsa — muvaffaqiyatli.
