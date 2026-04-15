"""Tanlangan kompaniya: xotira + PostgreSQL (bot qayta ishga tushganda ham saqlanadi)."""

_user_companies: dict[int, str] = {}


def set_company(user_id: int, company: str) -> None:
    company = (company or "").strip()
    _user_companies[user_id] = company
    try:
        from database.db import get_db

        db = get_db()
        if db and getattr(db, "connection", None):
            db.set_user_company(user_id, company)
    except Exception:
        pass


def get_company(user_id: int) -> str | None:
    c = _user_companies.get(user_id)
    if c:
        return str(c).strip()
    try:
        from database.db import get_db

        db = get_db()
        if db and getattr(db, "connection", None):
            c = db.get_user_company(user_id)
            if c:
                c = str(c).strip()
                _user_companies[user_id] = c
                return c
    except Exception:
        pass
    return None
