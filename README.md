# Load Bot

Telegram bot for managing logistics load board via Google Sheets.

## Features
- **Factoring Payments**: Scan invoices and update "Invoiced Amount" in Google Sheets.
- **Broker Payments**: Scan payment proofs and update "Broker Paid".
- **Expenses**: Auto-add Fuel and Toll expenses to respective sheets.
- **Statement Check**: Compare carrier statements with Google Sheet data to find mismatches.
- **Dynamic Sheet Selection**: Automatically selects the correct weekly sheet based on dates in the uploaded file.

## Setup

1.  **Requirements**: Python 3.9+, PostgreSQL.
2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
    Optional extras:
    ```bash
    # OCR/scanned PDF support
    pip install -r requirements-ocr.txt

    # Production reliability/observability helpers
    pip install -r requirements-prod.txt
    ```
3.  **Environment Variables**:
    Create a `.env` file based on `.env.example`:
    -   `BOT_TOKEN`: Telegram bot token.
    -   `GOOGLE_SHEETS_CREDENTIALS_JSON`: Path to service account JSON.
    -   `DATABASE_URL`: Postgres connection string.
4.  **Database**:
    Ensure PostgreSQL is running. The bot will create tables on first run.

## Usage
Run the bot:
```bash
python main.py
```

## Folder Structure
-   `handlers/`: Bot command handlers.
-   `services/`: Logic for Google Sheets, Excel parsing.
-   `database/`: DB connection and models.
-   `keyboards/`: Menu buttons.
