"""
Dostup boshqaruvi: PostgreSQL bazada. Faqat ruxsat berilganlar botdan foydalana oladi.
1 odam 10 daqiqa, keyin 2 daqiqa pauza, keyin navbatdagi.
"""
from datetime import datetime, timezone
from database.db import get_db

def _now_utc():
    """UTC vaqt - DB va Python solishtirish uchun."""
    return datetime.now(timezone.utc)

SESSION_MINUTES = 10
COOLDOWN_MINUTES = 2

def _db():
    return get_db()

def _cursor():
    db = _db()
    return db.cursor if db and db.connection else None

def get_all_admin_ids() -> list:
    """Barcha admin ID lar: .env + DB dan."""
    env_ids = [str(a).strip() for a in (getattr(__import__("config"), "ADMINS", []) or []) if a]
    cur = _cursor()
    if not cur:
        return env_ids
    try:
        cur.execute("SELECT user_id FROM admins")
        rows = cur.fetchall()
        db_ids = [str(r["user_id"]) for r in rows if r.get("user_id")]
        return list(dict.fromkeys(env_ids + db_ids))  # Unikal, tartibi saqlanadi
    except Exception:
        return env_ids


def is_admin(user_id: int, admin_ids: list = None) -> bool:
    """admin_ids berilmasa, get_all_admin_ids() ishlatiladi."""
    if admin_ids is None:
        admin_ids = get_all_admin_ids()
    return str(user_id) in [str(a).strip() for a in admin_ids if a]


def grant_admin(user_id: int, username: str = "", full_name: str = "") -> bool:
    cur = _cursor()
    if not cur:
        return False
    try:
        db = _db()
        db.cursor.execute("""
            INSERT INTO admins (user_id, username, full_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
            username = EXCLUDED.username,
            full_name = EXCLUDED.full_name
        """, (user_id, username or "", full_name or ""))
        db.connection.commit()
        return True
    except Exception as e:
        print(f"grant_admin error: {e}")
        return False


def revoke_admin(user_id: int) -> bool:
    cur = _cursor()
    if not cur:
        return False
    try:
        db = _db()
        db.cursor.execute("DELETE FROM admins WHERE user_id = %s", (user_id,))
        db.connection.commit()
        return db.cursor.rowcount > 0
    except Exception:
        return False


def get_admin_list() -> list:
    """(user_id, username, full_name) ro'yxati. .env + DB birlashtirilgan."""
    env = getattr(__import__("config"), "ADMINS", []) or []
    result = []
    seen = set()
    for aid in env:
        a = str(aid).strip()
        if not a or a in seen:
            continue
        seen.add(a)
        try:
            result.append((int(a), "", f"ID:{a} (.env)"))
        except ValueError:
            continue
    cur = _cursor()
    if cur:
        try:
            cur.execute("SELECT user_id, username, full_name FROM admins ORDER BY added_at")
            for r in cur.fetchall():
                uid = r.get("user_id")
                if uid and str(uid) not in seen:
                    seen.add(str(uid))
                    result.append((int(uid), r.get("username") or "", r.get("full_name") or ""))
        except Exception:
            pass
    return result

def has_access(user_id: int, admin_ids: list = None) -> bool:
    if is_admin(user_id, admin_ids or get_all_admin_ids()):
        return True
    cur = _cursor()
    if not cur: return False
    try:
        if not _get_setting("global_enabled", "true").lower() in ("true", "1"):
            return False
        cur.execute("SELECT 1 FROM allowed_users WHERE user_id = %s", (user_id,))
        return cur.fetchone() is not None
    except Exception:
        return False

def grant_access(user_id: int, username: str = "", full_name: str = "") -> bool:
    cur = _cursor()
    if not cur: return False
    try:
        db = _db()
        db.cursor.execute("""
            INSERT INTO allowed_users (user_id, username, full_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
            username = EXCLUDED.username,
            full_name = EXCLUDED.full_name
        """, (user_id, username or "", full_name or ""))
        db.connection.commit()
        return True
    except Exception as e:
        print(f"grant_access error: {e}")
        return False

def revoke_access(user_id: int) -> bool:
    cur = _cursor()
    if not cur: return False
    try:
        db = _db()
        db.cursor.execute("DELETE FROM allowed_users WHERE user_id = %s", (user_id,))
        db.connection.commit()
        return db.cursor.rowcount > 0
    except Exception:
        return False

def get_allowed_count() -> int:
    cur = _cursor()
    if not cur: return 0
    try:
        cur.execute("SELECT COUNT(*) as c FROM allowed_users")
        r = cur.fetchone()
        return r["c"] if r else 0
    except Exception:
        return 0

def get_allowed_list() -> list:
    cur = _cursor()
    if not cur: return []
    try:
        cur.execute("SELECT user_id, username, full_name FROM allowed_users ORDER BY added_at")
        rows = cur.fetchall()
        return [(int(r["user_id"]), r["username"] or "", r["full_name"] or "") for r in rows]
    except Exception:
        return []

def _get_setting(key: str, default: str = "") -> str:
    cur = _cursor()
    if not cur: return default
    try:
        cur.execute("SELECT value FROM access_settings WHERE key = %s", (key,))
        r = cur.fetchone()
        return r["value"] if r and r["value"] else default
    except Exception:
        return default

def _set_setting(key: str, value: str) -> None:
    cur = _cursor()
    if not cur: return
    try:
        db = _db()
        db.cursor.execute("""
            INSERT INTO access_settings (key, value, updated_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
        """, (key, value))
        db.connection.commit()
    except Exception:
        pass

def set_global_enabled(enabled: bool) -> None:
    _set_setting("global_enabled", "true" if enabled else "false")

def is_global_enabled() -> bool:
    return _get_setting("global_enabled", "true").lower() in ("true", "1")

def get_active_user() -> tuple[int | None, datetime | None]:
    cur = _cursor()
    if not cur: return None, None
    try:
        cur.execute("SELECT user_id, started_at FROM active_session WHERE id = 1")
        r = cur.fetchone()
        if not r or not r["user_id"]:
            return None, None
        started = r["started_at"]
        if started is None:
            return None, None
        if isinstance(started, str):
            started = datetime.fromisoformat(started.replace("Z", "+00:00"))
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        return int(r["user_id"]), started
    except Exception:
        return None, None

def set_active_user(user_id: int) -> None:
    cur = _cursor()
    if not cur: return
    try:
        db = _db()
        now = _now_utc()
        db.cursor.execute("""
            UPDATE active_session SET user_id = %s, started_at = %s
            WHERE id = 1
        """, (user_id, now))
        db.connection.commit()
    except Exception:
        pass

def add_to_queue(user_id: int) -> None:
    cur = _cursor()
    if not cur: return
    try:
        db = _db()
        db.cursor.execute("SELECT 1 FROM access_queue WHERE user_id = %s", (user_id,))
        if db.cursor.fetchone():
            return
        db.cursor.execute("INSERT INTO access_queue (user_id) VALUES (%s)", (user_id,))
        db.connection.commit()
    except Exception:
        pass

def clear_active_user() -> int | None:
    cur = _cursor()
    if not cur: return None
    try:
        db = _db()
        db.cursor.execute("SELECT user_id FROM access_queue ORDER BY id LIMIT 1")
        r = db.cursor.fetchone()
        next_id = int(r["user_id"]) if r and r.get("user_id") else None
        db.cursor.execute("""
            UPDATE active_session SET user_id = NULL, started_at = NULL, last_finish = %s
            WHERE id = 1
        """, (_now_utc(),))
        if next_id:
            db.cursor.execute("DELETE FROM access_queue WHERE user_id = %s", (next_id,))
        db.connection.commit()
        return next_id
    except Exception as e:
        print(f"clear_active_user error: {e}")
        return None

def get_last_finish() -> datetime | None:
    cur = _cursor()
    if not cur: return None
    try:
        cur.execute("SELECT last_finish FROM active_session WHERE id = 1")
        r = cur.fetchone()
        if not r or not r["last_finish"]:
            return None
        lf = r["last_finish"]
        if lf and hasattr(lf, 'tzinfo') and lf.tzinfo is None:
            lf = lf.replace(tzinfo=timezone.utc)
        return lf
    except Exception:
        return None

def can_start_session(user_id: int, admin_ids: list) -> tuple[bool, str]:
    if is_admin(user_id, admin_ids):
        return True, ""

    active_uid, started_at = get_active_user()
    last_finish = get_last_finish()

    if active_uid is None:
        if last_finish:
            elapsed = (_now_utc() - last_finish).total_seconds()
            if elapsed < COOLDOWN_MINUTES * 60:
                wait_sec = int(COOLDOWN_MINUTES * 60 - elapsed)
                return False, f"⏳ Navbatdagi sessiya uchun {wait_sec} soniya kutish kerak."
        return True, ""

    if active_uid == user_id:
        return True, ""

    add_to_queue(user_id)
    return False, "Boshqa foydalanuvchi botni ishlatayapti. U o'z vazifasini tugatgandan keyin 2 daqiqadan so'ng ishlata olasiz."

def check_session_timeout(user_id: int, admin_ids: list) -> tuple[bool, str, int | None]:
    if is_admin(user_id, admin_ids):
        return True, "", None

    active_uid, started_at = get_active_user()
    if active_uid != user_id:
        return True, "", None

    if not started_at:
        return True, "", None

    elapsed = (_now_utc() - started_at).total_seconds()
    if elapsed >= SESSION_MINUTES * 60:
        next_id = clear_active_user()
        return False, "⏰ Sizning vaqtingiz tugadi. Navbatda turing, keyin yana ishlatishingiz mumkin.", next_id

    return True, "", None

def get_active_user_display_name() -> str:
    active_uid, _ = get_active_user()
    if not active_uid:
        return "Hech kim"
    users = get_allowed_list()
    for uid, username, full_name in users:
        if uid == active_uid:
            return full_name or username or f"ID:{active_uid}"
    return f"ID:{active_uid}"
