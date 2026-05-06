import psycopg2
from psycopg2.extras import RealDictCursor
import config

_db_instance = None

def get_db():
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance

class Database:
    def __init__(self):
        self.db_url = None
        self.connection = None
        self.cursor = None
        if not getattr(config, "DATABASE_URL", None):
            print("Database connection failed: DATABASE_URL .env da yo'q")
            return
        self.db_url = config.DATABASE_URL
        if self.db_url.startswith("postgres://"):
            self.db_url = self.db_url.replace("postgres://", "postgresql://", 1)
        self._connect()
        self.create_tables()
    
    def _connect(self):
        try:
            self.connection = psycopg2.connect(
                self.db_url,
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            self.connection.autocommit = False
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            print("Database connected successfully.")
        except Exception as e:
            print(f"Database connection failed: {e}")
            self.connection = None
            self.cursor = None
    
    def ensure_connection(self):
        try:
            if self.connection and getattr(self.connection, "closed", 1) == 0:
                return True
        except Exception:
            pass
        self._connect()
        return bool(self.connection and getattr(self.connection, "closed", 1) == 0)
    
    def ensure_cursor(self):
        if not self.ensure_connection():
            return False
        try:
            if self.cursor and getattr(self.cursor, "closed", 1) == 0:
                return True
        except Exception:
            pass
        try:
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            return True
        except Exception as e:
            print(f"Cursor recreation failed: {e}")
            self.connection = None
            self.cursor = None
            return False

    def create_tables(self):
        if not self.ensure_cursor():
            return
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
            # Tanlangan Load / kompaniya (bot qayta ishga tushganda ham eslab qoladi)
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_company (
                    user_id BIGINT PRIMARY KEY,
                    company VARCHAR(100) NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.connection.commit()
        except Exception as e:
            print(f"Error creating tables: {e}")
            if self.connection:
                self.connection.rollback()

    def add_log(self, user_id, action, details=None, result=None, username=None, full_name=None):
        if not self.ensure_cursor():
            return
        try:
            self.cursor.execute("""
                INSERT INTO activity_logs (user_id, username, full_name, action, details, result)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (user_id, username, full_name, action, details, result))
            self.connection.commit()
        except Exception as e:
            print(f"Error adding log: {e}")
            if self.connection:
                self.connection.rollback()

    def get_recent_logs(self, limit=50):
        if not self.ensure_cursor():
            return []
        try:
            self.cursor.execute("""
                SELECT id, timestamp, user_id, username, full_name, action, details, result
                FROM activity_logs ORDER BY timestamp DESC LIMIT %s
            """, (limit,))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error getting logs: {e}")
            return []

    def get_users_with_activity(self, env_admin_ids=None):
        """Faqat hozir dostupi bor foydalanuvchilar (allowed_users, admins, .env ADMINS)."""
        if not self.ensure_cursor():
            return []
        try:
            env_ids = [int(x) for x in (env_admin_ids or []) if str(x).strip().isdigit()]
            if env_ids:
                placeholders = ",".join(["%s"] * len(env_ids))
                self.cursor.execute(f"""
                    SELECT DISTINCT ON (a.user_id) a.user_id, a.full_name, a.username
                    FROM activity_logs a
                    WHERE a.user_id IS NOT NULL
                      AND (
                        a.user_id IN (SELECT user_id FROM allowed_users)
                        OR a.user_id IN (SELECT user_id FROM admins)
                        OR a.user_id IN ({placeholders})
                      )
                    ORDER BY a.user_id, a.timestamp DESC
                """, env_ids)
            else:
                self.cursor.execute("""
                    SELECT DISTINCT ON (a.user_id) a.user_id, a.full_name, a.username
                    FROM activity_logs a
                    WHERE a.user_id IS NOT NULL
                      AND (
                        a.user_id IN (SELECT user_id FROM allowed_users)
                        OR a.user_id IN (SELECT user_id FROM admins)
                      )
                    ORDER BY a.user_id, a.timestamp DESC
                """)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error get_users_with_activity: {e}")
            return []

    def get_logs_by_user(self, user_id, limit=5000):
        """Foydalanuvchi barcha aktivliklari (dostup berilgandan beri)."""
        if not self.ensure_cursor():
            return []
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

    def set_user_company(self, user_id: int, company: str) -> None:
        if not self.ensure_cursor():
            return
        try:
            self.cursor.execute(
                """
                INSERT INTO user_company (user_id, company, updated_at)
                VALUES (%s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (user_id) DO UPDATE SET
                    company = EXCLUDED.company,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (user_id, company),
            )
            self.connection.commit()
        except Exception as e:
            print(f"set_user_company error: {e}")
            self.connection.rollback()

    def get_user_company(self, user_id: int):
        if not self.ensure_cursor():
            return None
        try:
            self.cursor.execute(
                "SELECT company FROM user_company WHERE user_id = %s",
                (user_id,),
            )
            row = self.cursor.fetchone()
            return row["company"] if row else None
        except Exception as e:
            print(f"get_user_company error: {e}")
            return None

    def close(self):
        if self.cursor:
            try:
                self.cursor.close()
            except Exception:
                pass
            self.cursor = None
        if self.connection:
            try:
                self.connection.close()
            except Exception:
                pass
            self.connection = None
