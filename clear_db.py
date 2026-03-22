"""Bazani tozalash: barcha foydalanuvchi ma'lumotlari, aktivlik loglari."""
import sys
from database.db import get_db

def main():
    db = get_db()
    if not db or not db.connection:
        print("Bazaga ulanish xatosi.")
        sys.exit(1)
    try:
        db.cursor.execute("TRUNCATE activity_logs RESTART IDENTITY CASCADE")
        db.cursor.execute("DELETE FROM allowed_users")
        db.cursor.execute("DELETE FROM admins")
        db.cursor.execute("DELETE FROM access_queue")
        db.cursor.execute("UPDATE active_session SET user_id = NULL, started_at = NULL, last_finish = NULL WHERE id = 1")
        db.connection.commit()
        print("Baza tozalandi: activity_logs, allowed_users, admins, access_queue.")
    except Exception as e:
        print(f"Xatolik: {e}")
        db.connection.rollback()
        sys.exit(1)

if __name__ == "__main__":
    main()
