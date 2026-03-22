import psycopg2
from psycopg2.extras import RealDictCursor
import config
from datetime import datetime

_db_instance = None

def get_db():
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance

class Database:
    def __init__(self):
        if not getattr(config, "DATABASE_URL", None):
            print("Database connection failed: DATABASE_URL .env da yo'q")
            self.connection = None
            self.cursor = None
            return
        try:
            db_url = config.DATABASE_URL
            if db_url.startswith("postgres://"):
                db_url = db_url.replace("postgres://", "postgresql://", 1)
            self.connection = psycopg2.connect(db_url)
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            self.create_tables()
            print("Database connected successfully.")
        except Exception as e:
            print(f"Database connection failed: {e}")
            self.connection = None
            self.cursor = None

    def create_tables(self):
        if not self.connection: return
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    full_name VARCHAR(255),
                    role VARCHAR(50) DEFAULT 'user',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS activity_logs (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    user_id BIGINT,
                    username VARCHAR(255),
                    full_name VARCHAR(255),
                    action VARCHAR(255),
                    details TEXT,
                    result VARCHAR(100)
                )
            """)
            # Dostup berilgan foydalanuvchilar
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    full_name VARCHAR(255),
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Global dostup sozlamasi
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS access_settings (
                    key VARCHAR(100) PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Aktiv sessiya (1 qator)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS active_session (
                    id INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                    user_id BIGINT,
                    started_at TIMESTAMP,
                    last_finish TIMESTAMP
                )
            """)
            self.cursor.execute("""
                INSERT INTO active_session (id, user_id, started_at, last_finish)
                VALUES (1, NULL, NULL, NULL)
                ON CONFLICT (id) DO NOTHING
            """)
            # Qo'shimcha adminlar (bot orqali qo'shilgan)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    user_id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    full_name VARCHAR(255),
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Navbat
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS access_queue (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.connection.commit()
        except Exception as e:
            print(f"Error creating tables: {e}")
            self.connection.rollback()

    def add_log(self, user_id, action, details=None, result=None, username=None, full_name=None):
        if not self.connection: return
        try:
            self.cursor.execute("""
                INSERT INTO activity_logs (user_id, username, full_name, action, details, result)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (user_id, username, full_name, action, details, result))
            self.connection.commit()
        except Exception as e:
            print(f"Error adding log: {e}")

    def get_recent_logs(self, limit=50):
        if not self.connection: return []
        try:
            self.cursor.execute("""
                SELECT id, timestamp, user_id, username, full_name, action, details, result
                FROM activity_logs ORDER BY timestamp DESC LIMIT %s
            """, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error getting logs: {e}")
            return []

    def get_users_with_activity(self):
        """Aktivlik qilgan foydalanuvchilar (buxgalters va adminlar)."""
        if not self.connection: return []
        try:
            self.cursor.execute("""
                SELECT DISTINCT ON (user_id) user_id, full_name, username
                FROM activity_logs
                WHERE user_id IS NOT NULL
                ORDER BY user_id, timestamp DESC
            """)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error get_users_with_activity: {e}")
            return []

    def get_logs_by_user(self, user_id, limit=5000):
        """Foydalanuvchi barcha aktivliklari (dostup berilgandan beri)."""
        if not self.connection: return []
        try:
            self.cursor.execute("""
                SELECT timestamp, action, details, result, username, full_name
                FROM activity_logs
                WHERE user_id = %s
                ORDER BY timestamp ASC
            """, (user_id,))
            return self.cursor.fetchall()[:limit]
        except Exception as e:
            print(f"Error get_logs_by_user: {e}")
            return []

    def close(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
