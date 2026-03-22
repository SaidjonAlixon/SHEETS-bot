"""Tanlangan kompaniya saqlash (user_id -> company). State clear qilinsa ham saqlanadi."""

_user_companies: dict[int, str] = {}

def set_company(user_id: int, company: str) -> None:
    _user_companies[user_id] = company

def get_company(user_id: int) -> str | None:
    return _user_companies.get(user_id)
