"""
Company Driver settlement PDF dan haydovchi, foiz, ish davri va trip/reytinglarini ajratish.
Turli PDF generatorlar (TurboTax, payroll va h.k.) uchun bir nechta ajratish strategiyasi.
"""
from __future__ import annotations

import io
import json
import re
import urllib.error
import urllib.request
from datetime import date, datetime
from typing import Any

import pdfplumber
import config

_MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

# pdfplumber jadval strategiyalari (ketma-ket sinash)
_TABLE_SETTINGS = (
    {},
    {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "intersection_tolerance": 8,
        "snap_tolerance": 4,
    },
    {
        "vertical_strategy": "text",
        "horizontal_strategy": "text",
        "intersection_tolerance": 10,
        "text_tolerance": 3,
    },
)


def _month_day(mon: str, day: str, year: int) -> date | None:
    mi = _MONTHS.get((mon or "")[:3].lower())
    if not mi:
        return None
    try:
        return date(year, mi, int(day))
    except ValueError:
        return None


def _clean_money(val: str | None) -> float | None:
    if val is None:
        return None
    s = str(val).replace("$", "").replace(",", "").strip()
    if not s or s.lower() == "nan":
        return None
    m = re.search(r"-?\d+\.?\d*", s)
    if not m:
        return None
    try:
        return float(m.group())
    except ValueError:
        return None


def _is_valid_load_id(token: str | None) -> bool:
    """
    Load ID uchun qattiq filtr:
    - M-prefiksli: M12345...
    - Yoki kamida 6 xonali raqam (5 xonali trip no emas)
    """
    if not token:
        return False
    s = str(token).strip().upper()
    if not s:
        return False
    if re.fullmatch(r"M\d{5,}", s):
        return True
    # Harf-dash-raqam (masalan L-17906)
    if re.fullmatch(r"[A-Z]-\d{4,10}", s):
        return True
    # Raqamli dashed ID (masalan 31448-34015)
    if re.fullmatch(r"\d{4,10}(?:-\d{3,10}){1,2}", s):
        return True
    # Ba'zi boardlarda load id 5 xonali yoki #008624 ko'rinishida bo'lishi mumkin
    if re.fullmatch(r"#?\d{5,10}", s):
        return True
    # Alfanumerik prefiksli IDlar (masalan A123456)
    if re.fullmatch(r"[A-Z]\d{5,10}", s):
        return True
    # Aralash harf/raqam ID (masalan 1LLEOEW2540, AB12CD3456)
    compact = re.sub(r"[^A-Z0-9]", "", s)
    if (
        len(compact) >= 6
        and re.search(r"[A-Z]", compact)
        and re.search(r"\d", compact)
        and re.fullmatch(r"[A-Z0-9]+", compact)
    ):
        return True
    return False


def _extract_gross_rate_from_cell(cell: Any) -> float | None:
    """
    Rate (Gross) katagidan aynan gross summani oladi:
    - odatda birinchi qatordagi katta summa ($2,800.00)
    - pastdagi $/mi (masalan $2.473 /mi) ni hisobga olmaydi
    """
    if cell is None:
        return None
    raw = str(cell).replace("\r", "\n")
    # Qat'iy qoida:
    # - Rate (Gross) katagidagi birinchi (yuqoridagi) summa olinadi
    # - /mi bo'lgan unit-rate summalar e'tiborga olinmaydi
    lines = [x.strip() for x in raw.split("\n") if x.strip()]
    for line in lines:
        segment = line
        low = line.lower()
        if "/mi" in low:
            # /mi dan keyingi qiymatlar unit-rate, gross emas.
            segment = line[: low.index("/mi")]
        # Faqat 2 xonali decimal qiymatlarni olamiz; 2.918 kabi /mi qiymat tushmasin.
        vals = re.findall(r"(?:\$\s*)?[\d,]+\.\d{2}(?!\d)", segment)
        if not vals:
            continue
        first_val = _clean_money(vals[0])
        if first_val is not None and first_val >= 50:
            return first_val

    # Katakdan aniq gross topilmasa None qaytaramiz (noto'g'ri /mi olishdan ko'ra yaxshi).
    return None


def _trip_id_from_cell(cell: Any) -> str | None:
    if cell is None:
        return None
    raw = str(cell).replace("\r", "\n")
    lines = [x.strip() for x in re.split(r"[\n]+", raw) if x.strip()]
    if not lines:
        parts = re.split(r"\s+", raw.strip())
        lines = [p for p in parts if p]
    if not lines:
        return None

    def _repair_split_numeric_tokens(line: str) -> list[str]:
        """
        Ba'zi PDFlarda uzun load id 2 ta raqam bo'lib chiqadi (masalan: '5000' va '112074').
        Shunday bo'lsa, ularni birlashtirib bitta token qilib qaytaramiz.
        """
        s = str(line or "")
        if not s:
            return []
        # faqat raqam + bo'sh joy/punktuatsiya bo'lgan segmentlarni qidiramiz
        parts = re.findall(r"\d{3,6}", s)
        if len(parts) < 2:
            return []
        out: list[str] = []
        i = 0
        while i < len(parts):
            a = parts[i]
            # Agarda keyingi qism bilan birlashtirganda load id bo'lsa
            if i + 1 < len(parts):
                b = parts[i + 1]
                merged = f"{a}{b}"
                if _is_valid_load_id(merged):
                    out.append(merged)
                    i += 2
                    continue
            # yoki 3 ta bo'linma (kamdan-kam)
            if i + 2 < len(parts):
                c = parts[i + 2]
                merged3 = f"{a}{b}{c}"
                if _is_valid_load_id(merged3):
                    out.append(merged3)
                    i += 3
                    continue
            i += 1
        return out

    def _line_tokens_for_load(line: str) -> list[str]:
        toks: list[str] = []
        toks.extend(_repair_split_numeric_tokens(line))
        for m in re.finditer(r"\b([A-Z]-\d{4,10})\b", line.upper()):
            toks.append(m.group(1).upper())
        for m in re.finditer(r"\b(\d{4,10}(?:-\d{3,10}){1,2})\b", line):
            toks.append(m.group(1).upper())
        for m in re.finditer(r"\b(M\d{5,})\b", line, re.I):
            toks.append(m.group(1).upper())
        for m in re.finditer(r"(#?\d{5,10})", line):
            toks.append(m.group(1))
        for m in re.finditer(r"\b([A-Z0-9]{6,16})\b", line.upper()):
            tok = m.group(1).upper()
            if re.search(r"[A-Z]", tok) and re.search(r"\d", tok):
                toks.append(tok)
        out: list[str] = []
        seen: set[str] = set()
        for t in toks:
            if t not in seen and _is_valid_load_id(t):
                seen.add(t)
                out.append(t)
        return out

    # Qat'iy qoida: ikki qatorli katakda 1-qator qizil trip no bo'lsa,
    # 2-qator (pastdagi yashil) load id olinadi.
    if len(lines) >= 2:
        first = lines[0].strip()
        second = lines[1].strip()
        first_is_trip = bool(re.fullmatch(r"#?\d{5}", first)) or _looks_internal_trip_number(first)
        second_tokens = _line_tokens_for_load(second)
        if first_is_trip and second_tokens:
            return second_tokens[0]

    # Rangni bevosita o'qib bo'lmagani uchun heuristika:
    # - 1-qatordagi trip no (qizil, ko'pincha 5 xonali) ni e'tiborsiz qoldirish
    # - pastki qatordagi LOAD ID (yashil) ni ustun qo'yish
    # - 6+ xonali yoki harf+raqam ID 5 xonali trip no'dan ustun
    candidates: list[tuple[int, str]] = []
    has_multiple_lines = len(lines) > 1
    # Multi-line katakda pastki qatorlarni oldin tekshiramiz (yashil load id).
    ordered_lines: list[tuple[int, str]] = []
    if has_multiple_lines:
        for li in range(1, len(lines)):
            ordered_lines.append((li, lines[li]))
        ordered_lines.append((0, lines[0]))
    else:
        ordered_lines.append((0, lines[0]))
    for li, line in ordered_lines:
        # Dashed numeric load id (masalan 31448-34015)
        for m in re.finditer(r"\b(\d{4,10}(?:-\d{3,10}){1,2})\b", line):
            tok = m.group(1).upper()
            score = 140 + li * 20
            candidates.append((score, tok))
        # Harf-dash-raqam ID (masalan L-17906)
        for m in re.finditer(r"\b([A-Z]-\d{4,10})\b", line.upper()):
            tok = m.group(1).upper()
            score = 145 + li * 20
            candidates.append((score, tok))
        # Aralash alfanumerik ID (ko'pincha yashil LOAD ID shu formatda keladi)
        for m in re.finditer(r"\b([A-Z0-9]{6,16})\b", line.upper()):
            tok = m.group(1).upper()
            if not (re.search(r"[A-Z]", tok) and re.search(r"\d", tok)):
                continue
            score = 130 + li * 20
            candidates.append((score, tok))
        # M-prefiksli ID
        for m in re.finditer(r"\b(M\d{5,})\b", line, re.I):
            tok = m.group(1).upper()
            score = 100 + li * 20
            candidates.append((score, tok))
        # Raqamli ID
        for m in re.finditer(r"(#?\d{5,10})", line):
            tok = m.group(1)
            score = 40 + li * 20
            if len(tok) >= 6:
                score += 20
            # Ko'p PDFlarda qizil trip no 70xxx bo'ladi
            if len(tok.lstrip("#")) == 5 and tok.lstrip("#").startswith("70"):
                score -= 35
            # Multi-line katakda 1-qator 5 xonali bo'lsa, bu odatda trip no.
            if has_multiple_lines and li == 0 and len(tok.lstrip("#")) == 5:
                score -= 80
            # Pastki qatordagi 6+ xonali IDlar (masalan 2847062) ustun bo'lsin.
            if has_multiple_lines and li > 0 and len(tok.lstrip("#")) >= 6:
                score += 45
            candidates.append((score, tok))

    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        for _, tok in candidates:
            if _is_valid_load_id(tok):
                return tok
    return None


def _find_driver_name(text: str) -> str | None:
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    street_re = re.compile(
        r"\d{2,5}\s+[A-Za-z0-9#'\s\-]+(?:DR|ST|AVE|RD|BLVD|LN|WAY|CT|CIR|PL|HWY)\b",
        re.I,
    )
    for i, line in enumerate(lines[:50]):
        if street_re.search(line) and i > 0:
            prev = lines[i - 1]
            if re.match(
                r"^[A-Za-z][a-zA-Z'\-]+(?:\s+[A-Za-z][a-zA-Z'\-]+){1,4}$",
                prev,
            ):
                if not re.search(r"\b(LLC|INC|Corp|Transport)\b", prev, re.I):
                    return prev
    skip_sub = (
        "llc",
        "inc",
        "transport",
        "company driver",
        "truck",
        "percent",
        "payroll",
        "work period",
        "total trip",
        "payout",
        "delo ",
        "phone",
        "fax",
        "www.",
        "@",
        "driver payroll",
    )
    for line in lines[:40]:
        low = line.lower()
        if any(s in low for s in skip_sub):
            continue
        if re.search(r"\d{3}[-.\s]?\d{3}[-.\s]?\d{4}", line):
            continue
        if re.match(
            r"^[A-Za-z][a-zA-Z'\-]+(?:\s+[A-Za-z][a-zA-Z'\-]+){1,4}$",
            line,
        ):
            if re.search(r"\b(FL|CA|TX|OH|NY|NJ|GA|IL|AZ|NV|WA|PA)\b\s*$", line):
                continue
            return line
    return None


def _parse_work_period(text: str) -> tuple[date | None, date | None, str | None]:
    mon = r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
    # Mar 23 - Mar 29, 2026 yoki Mar 23-Mar 29, 2026
    m = re.search(
        rf"{mon}\s+(\d{{1,2}})\s*[-–—]\s*{mon}\s+(\d{{1,2}}),?\s*(\d{{4}})",
        text,
        re.I,
    )
    if not m:
        return None, None, None
    m1, d1, m2, d2, y = m.group(1), m.group(2), m.group(3), m.group(4), int(m.group(5))
    raw = m.group(0)
    start = _month_day(m1, d1, y)
    end = _month_day(m2, d2, y)
    if start and end and end < start:
        end = _month_day(m2, d2, y + 1)
    return start, end, raw


def _pick_sheet_anchor_date(start: date | None, end: date | None) -> date | None:
    if start and end:
        return start + (end - start) // 2
    return start or end


def _norm_cell(c: Any) -> str:
    return str(c or "").lower().replace("\n", " ").strip()


def _parse_trips_from_table_strict(table: list[list[Any | None]]) -> list[dict[str, Any]]:
    """Eski mantiq: bitta qatorda trips + rate sarlavhasi."""
    if not table:
        return []
    header_row_idx = None
    trips_ci = None
    rate_ci = None
    for ri, row in enumerate(table):
        if not row:
            continue
        cells = [_norm_cell(c) for c in row]
        joined = " ".join(cells)
        if "trips" in joined and ("rate" in joined or "gross" in joined or "origin" in joined):
            header_row_idx = ri
            for ci, cell in enumerate(row):
                c = _norm_cell(cell)
                if c == "trips" or (c.startswith("trips") and "origin" not in c):
                    trips_ci = ci
                if "rate" in c and "gross" in c:
                    rate_ci = ci
                elif "rate" in c and rate_ci is None and "net" not in c:
                    rate_ci = ci
            break
    if header_row_idx is None or trips_ci is None or rate_ci is None:
        return []

    out: list[dict[str, Any]] = []
    for row in table[header_row_idx + 1 :]:
        if not row:
            continue

        def cell(i: int) -> Any:
            return row[i] if i < len(row) else None

        tid = _trip_id_from_cell(cell(trips_ci))
        rate = _extract_gross_rate_from_cell(cell(rate_ci))
        if not tid and rate is None:
            continue
        if tid and rate is not None:
            out.append({"trip_id": tid, "rate_gross": rate})
    return out


def _parse_trips_from_table_loose(table: list[list[Any | None]]) -> list[dict[str, Any]]:
    """
    Sarlavha bir necha qator yoki 'Trip' / 'Rate (Gross)' bo'linib yotishi mumkin.
    """
    if not table:
        return []
    trips_ci = None
    rate_ci = None
    header_end = -1

    for ri, row in enumerate(table[:35]):
        if not row:
            continue
        row_t = None
        row_r = None
        for ci, cell in enumerate(row):
            c = _norm_cell(cell)
            if not c:
                continue
            if re.search(r"\btrips?\b", c) and "origin" not in c and "destination" not in c:
                if len(c) < 40:
                    row_t = ci
            if ("gross" in c and "net" not in c and "total" not in c) or "rate (gross)" in c:
                row_r = ci
            elif c in ("rate", "gross") and "net" not in c:
                row_r = ci
        if row_t is not None:
            trips_ci = row_t
        if row_r is not None:
            rate_ci = row_r
        if trips_ci is not None and rate_ci is not None:
            header_end = max(header_end, ri)

    if trips_ci is None or rate_ci is None or header_end < 0:
        return []

    out: list[dict[str, Any]] = []
    for row in table[header_end + 1 :]:
        if not row:
            continue
        joined = " ".join(_norm_cell(x) for x in row if x)
        if "total" in joined and ("trip" in joined or "gross" in joined or "mile" in joined):
            break

        def cell(i: int) -> Any:
            return row[i] if i < len(row) else None

        tid = _trip_id_from_cell(cell(trips_ci))
        rate = _extract_gross_rate_from_cell(cell(rate_ci))
        if not tid and rate is None:
            continue
        if tid and rate is not None:
            out.append({"trip_id": tid, "rate_gross": rate})
    return out


def _parse_trips_from_free_text(text: str) -> list[dict[str, Any]]:
    """
    Jadval ajratilmasa: matn qatorlarida M123456 va $2,800.00 qidirish.
    """
    lines = [l.strip() for l in text.replace("\r", "\n").split("\n")]
    out: list[dict[str, Any]] = []
    in_trips = False

    for line in lines:
        low = line.lower()
        if not in_trips:
            if low.strip() == "trips" or (
                "trip" in low
                and any(
                    k in low
                    for k in (
                        "origin",
                        "destination",
                        "mileage",
                        "rate",
                        "gross",
                        "contract",
                    )
                )
            ):
                in_trips = True
            continue

        if re.match(r"^total\s+trip\b", low) or re.match(r"^total\s+miles\b", low):
            break
        if low.startswith("payout") and "$" in line:
            break
        if not line or len(line) < 4:
            continue
        if low in ("trips", "trip", "origin", "destination"):
            continue

        tid = None
        for m in re.finditer(r"\b(M\d{5,})\b", line, re.I):
            tid = m.group(1).upper()
        if not tid:
            for m in re.finditer(r"(\d{4,10}(?:-\d{3,10}){1,2}|#?\d{5,10})", line):
                cand = m.group(1)
                clean_cand = cand.lstrip("#")
                if clean_cand not in ("202020", "202120", "202220", "202320", "202420", "202520", "202620"):
                    tid = cand

        # Faqat gross summa: /mi bo'lgan unit-rate qiymatni olmaymiz
        rate = _extract_gross_rate_from_cell(line)

        if tid and rate is not None and rate >= 50 and _is_valid_load_id(tid):
            out.append({"trip_id": tid, "rate_gross": rate})

    return out


def _parse_trips_whole_text_brute(text: str) -> list[dict[str, Any]]:
    """
    Oxirgi zaxira: butun matnda M123456 dan keyin yaqin $ summani juftlash.
    (Ba'zi PDF lar qatorlarni birlashtirib beradi.)
    """
    out: list[dict[str, Any]] = []
    for m in re.finditer(r"\b(M\d{5,})\b", text, re.I):
        tid = m.group(1).upper()
        tail = text[m.end() : m.end() + 500]
        rate = _extract_gross_rate_from_cell(tail)
        if rate is not None and rate >= 100 and _is_valid_load_id(tid):
            out.append({"trip_id": tid, "rate_gross": rate})
    return out


def _dedupe_trips(trips: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for t in trips:
        tid = t.get("trip_id")
        if not tid or tid in seen or not _is_valid_load_id(tid):
            continue
        seen.add(tid)
        out.append(t)
    return out


def _looks_internal_trip_number(token: str | None) -> bool:
    """
    Qizil Trips raqamini filtrlash:
    - odatda 5 xonali va ko'pincha 7 bilan boshlanadi (71425, 71533, ...)
    """
    if not token:
        return False
    s = str(token).strip().upper()
    s = re.sub(r"[^A-Z0-9-]", "", s)
    return bool(re.fullmatch(r"\d{5}", s) and s.startswith("7"))


def _merge_trip_lists(
    ai_trips: list[dict[str, Any]], base_trips: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """
    AI ba'zan hamma tripni bermasligi yoki qizil trip no berishi mumkin.
    Shuning uchun heuristicni ustun qo'yib, keyin AI qo'shimchalarini qo'shamiz.
    """
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source_idx, source in enumerate((base_trips or [], ai_trips or [])):
        for t in source:
            if not isinstance(t, dict):
                continue
            tid = str(t.get("trip_id") or "").strip()
            rate = _clean_money(t.get("rate_gross"))
            if not tid or rate is None:
                continue
            # AI source dan kelgan qizil 5 xonali trip numberlarni tashlab yuboramiz.
            if source_idx == 1 and _looks_internal_trip_number(tid):
                continue
            key = re.sub(r"\s+", "", tid).upper()
            if key in seen:
                continue
            seen.add(key)
            out.append({"trip_id": tid, "rate_gross": rate})
    return _dedupe_trips(out)


def _extract_section(text: str, start_label: str, end_labels: list[str]) -> str:
    if not text:
        return ""
    lines = [ln.rstrip() for ln in text.replace("\r", "\n").split("\n")]
    start_idx = -1
    for i, line in enumerate(lines):
        if start_label.lower() in line.lower():
            start_idx = i
            break
    if start_idx < 0:
        return ""
    end_idx = len(lines)
    for i in range(start_idx + 1, len(lines)):
        low = lines[i].lower().strip()
        if any(lbl.lower() in low for lbl in end_labels):
            end_idx = i
            break
    return "\n".join(lines[start_idx:end_idx]).strip()


def _extract_money_from_line(line: str) -> float | None:
    vals = re.findall(r"(?:\$\s*)?[\d,]+\.\d{2}", line or "")
    if not vals:
        return None
    return _clean_money(vals[-1])


def _extract_money_next_token(line: str, token: str) -> float | None:
    if not line:
        return None
    m = re.search(rf"{re.escape(token)}\s+\$?\s*([\d,]+\.\d{{2}})", line, re.I)
    if not m:
        return None
    return _clean_money(m.group(1))


def _extract_fuel_transactions(text: str) -> tuple[list[dict[str, Any]], float | None]:
    section = _extract_section(
        text,
        "Fuel Transaction",
        ["Toll Transaction", "Payout", "Deductions"],
    )
    if not section:
        return [], None
    entries: list[dict[str, Any]] = []
    total_amount = None
    sec = section.replace("\r", "\n")

    # 1) Totals blokidan pay amountni olish (ustuvor)
    totals_match = re.search(r"(?is)\btotals\s*:\s*(.{0,260})", sec)
    if totals_match:
        totals_block = totals_match.group(0)
        total_amount = _extract_money_next_token(totals_block, "pay amount")
        if total_amount is None:
            vals = re.findall(r"(?:\$\s*)?[\d,]+\.\d{2}", totals_block)
            if vals:
                total_amount = _clean_money(vals[-1])

    # 2) Tranzaksiya qatorlarini blok bo'yicha ajratish (date/time bo'yicha)
    # Sana va vaqt bir satrda bo'lmasligi mumkin, shuning uchun whitespace/newline tolerant.
    row_re = re.compile(
        r"(\d{1,2}/\d{1,2}/\d{4})\s*(?:\n|\s)+(\d{1,2}:\d{2}\s*(?:AM|PM))",
        re.I,
    )
    row_matches = list(row_re.finditer(sec))
    pay_amount_rows: list[float] = []
    cut_at = len(sec)
    totals_pos = re.search(r"(?i)\btotals\s*:", sec)
    if totals_pos:
        cut_at = totals_pos.start()

    for i, m in enumerate(row_matches):
        start = m.start()
        if start >= cut_at:
            continue
        end = row_matches[i + 1].start() if i + 1 < len(row_matches) else cut_at
        end = min(end, cut_at)
        chunk = sec[start:end]
        vals = re.findall(r"(?:\$\s*)?[\d,]+\.\d{2}", chunk)
        if not vals:
            continue
        pay = _clean_money(vals[-1])
        if pay is None:
            continue
        pay_amount_rows.append(float(pay))
        entries.append(
            {
                "type": "Diesel",
                "date_time": f"{m.group(1)} {m.group(2).upper()}",
                "pay_amount": pay,
                "raw": chunk.strip(),
            }
        )

    # 3) Agar totals topilmasa, Pay Amount ustunidagi jami bilan to'ldiramiz
    if total_amount is None and pay_amount_rows:
        total_amount = round(sum(pay_amount_rows), 2)
    if total_amount is None and entries:
        total_amount = round(sum(float(x.get("pay_amount") or 0) for x in entries), 2)
    return entries, total_amount


def _extract_toll_transactions(text: str) -> tuple[list[dict[str, Any]], float | None]:
    matches = re.finditer(r"(?im)^.*toll transaction.*$", text or "")
    starts = [m.start() for m in matches]
    if not starts:
        return [], None
    toll_blocks: list[str] = []
    for i, st in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(text)
        toll_blocks.append((text or "")[st:end])
    entries: list[dict[str, Any]] = []
    total_amount = None
    for block in toll_blocks:
        lines = block.replace("\r", "\n").split("\n")
        last_device_id: str | None = None
        for i, line in enumerate(lines):
            s = line.strip()
            if not s:
                continue
            low = s.lower()
            if "totals:" in low:
                lookahead = " ".join(
                    x.strip() for x in lines[i : min(i + 4, len(lines))] if str(x).strip()
                )
                line_total = _extract_money_from_line(lookahead)
                if line_total is not None:
                    total_amount = (total_amount or 0.0) + float(line_total)
                continue
            if "provider" in low and "device" in low and "pay amount" in low:
                continue
            # Provider nomi satr boshida bo'ladi: EZPass, ELITE va h.k.
            provider_match = re.match(r"^\s*([A-Za-z][A-Za-z0-9_-]{1,20})\b", s)
            if not provider_match:
                continue
            provider = provider_match.group(1)
            dev_match = re.search(r"\b(\d{6,})\b", s)
            if dev_match:
                last_device_id = dev_match.group(1)
            dt_match = re.search(
                r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2}\s*(?:AM|PM))", s, re.I
            )
            amount = _extract_money_from_line(s)
            # Ba'zi PDFlarda keyingi qatorlarda device id bo'sh keladi;
            # bunday holatda oldingi qatordagi device id ni davom ettiramiz.
            device_id = dev_match.group(1) if dev_match else last_device_id
            if device_id and dt_match and amount is not None:
                entries.append(
                    {
                        "provider": provider,
                        "device_id": device_id,
                        "exit_date_time": f"{dt_match.group(1)} {dt_match.group(2).upper()}",
                        "pay_amount": amount,
                        "raw": s,
                    }
                )
    if total_amount is None and entries:
        total_amount = round(sum(float(x.get("pay_amount") or 0) for x in entries), 2)
    return entries, total_amount


def _extract_rate_near_trip_id(text: str, trip_id: str) -> float | None:
    """
    PDF matnda trip/load ID atrofidan Rate (Gross) ni topishga urinadi.
    Asosiy qoida: ID dan keyin kelgan birinchi valyuta qiymati ($...) gross bo'lishi ehtimoli yuqori.
    Muhim: mileage kabi oddiy sonlarni (masalan 660.00) rate deb olmaymiz.
    """
    if not text or not trip_id:
        return None
    raw_tid = str(trip_id).strip()
    if not raw_tid:
        return None
    # MUHIM: substring qidiruv 5000112074 ichida 500011207 kabi qisqa bo'linmani ham ushlaydi
    # va keyingi $ summani noto'g'ri qator bilan bog'laydi. Shuning uchun to'liq token qidiramiz.
    tid_esc = re.escape(raw_tid)
    boundary = r"(?<![A-Z0-9#-])" + tid_esc + r"(?![A-Z0-9#-])"
    hits = list(re.finditer(boundary, text, re.I))
    if not hits:
        return None
    for m in hits:
        tail = text[m.end() : m.end() + 320]
        # Faqat $ bilan boshlangan qiymatlar — mileage bilan chalkashmasin
        vals = re.findall(r"\$\s*[\d,]+\.\d{2}", tail)
        nums: list[float] = []
        for v in vals:
            n = _clean_money(v)
            if n is None:
                continue
            # /mi unit-rate odatda kichik bo'ladi, gross esa kattaroq.
            if n >= 100:
                nums.append(float(n))
        if nums:
            return nums[0]
    return None


def _normalize_trip_rates_by_text(trips: list[dict[str, Any]], text: str) -> list[dict[str, Any]]:
    """
    Jadvaldan noto'g'ri tushgan rate larni to'g'rilash:
    har trip uchun PDF matndan ID yonidagi gross summani topib override qiladi.
    """
    out: list[dict[str, Any]] = []
    for t in trips or []:
        tid = str(t.get("trip_id") or "").strip()
        rate = _clean_money(t.get("rate_gross"))
        if not tid:
            continue
        near = _extract_rate_near_trip_id(text, tid)
        # Faqat aniqroq bo'lganda override qilamiz:
        # - current rate bo'sh bo'lsa yoki
        # - near qiymat valyuta asosida topilgan bo'lib currentdan sezilarli farq qilsa
        if near is not None and (rate is None or abs(float(rate) - float(near)) > 0.01):
            rate = near
        if rate is None:
            continue
        out.append({"trip_id": tid, "rate_gross": rate})
    return _dedupe_trips(out)


def _repair_suspicious_trip_rates(trips: list[dict[str, Any]], text: str) -> list[dict[str, Any]]:
    """
    Ba'zan AI/fallback `rate_gross` ni 89, 24, 9 kabi noto'g'ri mayda qiymat qilib yuboradi.
    Shunday hollarda load id atrofidan qayta gross ($...) topib tuzatamiz.
    """
    out: list[dict[str, Any]] = []
    for t in trips or []:
        tid = str(t.get("trip_id") or "").strip()
        rate = _clean_money(t.get("rate_gross"))
        if not tid or rate is None:
            continue
        # Real gross odatda ancha katta bo'ladi; 100 dan kichik bo'lsa shubhali deb olamiz.
        if float(rate) < 100:
            near = _extract_rate_near_trip_id(text, tid)
            if near is not None and float(near) >= 100:
                rate = near
        out.append({"trip_id": tid, "rate_gross": rate})
    return _dedupe_trips(out)


def _to_iso(d: date | None) -> str | None:
    return d.isoformat() if isinstance(d, date) else None


def _date_from_iso(s: str | None) -> date | None:
    if not s:
        return None
    try:
        y, m, d = [int(x) for x in str(s).split("-")]
        return date(y, m, d)
    except Exception:
        return None


def _extract_json_object(text: str) -> dict[str, Any] | None:
    if not text:
        return None
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    raw = text[start : end + 1]
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return None
    return None


def _call_openai_settlement_parser(text: str) -> dict[str, Any] | None:
    api_key = (config.OPENAI_API_KEY or "").strip()
    model = (config.OPENAI_MODEL or "").strip() or "gpt-4.1-mini"
    if not api_key or not text.strip():
        return None

    schema_hint = {
        "driver_name": "string",
        "percent": "number|null",
        "work_period_raw": "string|null",
        "work_period_start": "YYYY-MM-DD|null",
        "work_period_end": "YYYY-MM-DD|null",
        "trips": [{"trip_id": "string", "rate_gross": "number"}],
        "fuel_total_pay_amount": "number|null",
        "toll_total_pay_amount": "number|null",
    }
    prompt = (
        "Extract Company Driver settlement data from the text.\n"
        "Return only one JSON object, no markdown.\n"
        "Rules:\n"
        "- Read only explicit values from text.\n"
        "- trip_id must be each real LOAD id from the Trips row (if two loads, two trips entries).\n"
        "- In Trips cell, ignore top red trip number; pick the lower/second-line LOAD ID.\n"
        "- Load ID may be numeric, alphanumeric, or letter-dash-number (e.g. L-17906).\n"
        "- Never truncate long numeric load ids (e.g. 5000112074 must stay full length).\n"
        "- If a load id appears split across spaces/newlines, reconstruct it as one trip_id.\n"
        "- If first line is 5-digit trip number and second line is another id, always take second line as trip_id.\n"
        "- Ignore internal trip numbers if a separate load id exists on the same row.\n"
        "- rate_gross must be gross amount for that same row; ignore per-mile values (/mi).\n"
        "- Read rate_gross strictly from the 'Rate (Gross)' column, not from Net Amount.\n"
        "- In each Rate (Gross) cell, take only the first/top amount (green one), never the lower '/mi' amount.\n"
        "- Never use mileage values (e.g., 660.00, 1172.00) as rate_gross.\n"
        "- Prefer currency-formatted gross values ($...) for rate_gross.\n"
        "- fuel_total_pay_amount must be Fuel Transaction section TOTAL Pay Amount.\n"
        "- toll_total_pay_amount must be Toll Transaction section TOTAL Pay Amount.\n"
        "- Keep trips unique by trip_id.\n"
        "- Use null when unknown.\n"
        f"JSON shape: {json.dumps(schema_hint)}\n\n"
        "Document text:\n"
        f"{text[:140000]}"
    )
    body = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "You are a strict information extraction engine for payroll PDFs.",
                    }
                ],
            },
            {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
        ],
        "temperature": 0,
    }
    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=70) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        return None

    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return _extract_json_object(output_text)

    chunks = payload.get("output") or []
    for item in chunks:
        content = item.get("content") or []
        for c in content:
            txt = c.get("text")
            if isinstance(txt, str):
                parsed = _extract_json_object(txt)
                if parsed:
                    return parsed
    return None


def _extract_all_tables(pdf: Any) -> list[list[list[Any]]]:
    seen: set[str] = set()
    tables: list[list[list[Any]]] = []

    def add(tb: list[list[Any]] | None) -> None:
        if not tb:
            return
        key = repr(tb[:3])[:2000]
        if key in seen:
            return
        seen.add(key)
        tables.append(tb)

    for page in pdf.pages:
        for settings in _TABLE_SETTINGS:
            try:
                for tb in page.extract_tables(table_settings=settings) or []:
                    add(tb)
            except Exception:
                continue
        try:
            for tb in page.extract_tables() or []:
                add(tb)
        except Exception:
            pass
    return tables


def parse_company_driver_settlement_pdf(file_content: bytes) -> dict[str, Any]:
    """
    Qaytaradi:
      driver_name, percent (int|None), work_period_start, work_period_end, work_period_raw,
      anchor_date (sheet tanlash uchun), trips: [{trip_id, rate_gross}],
      parse_warnings: [str]
    """
    warnings: list[str] = []
    full_text_parts: list[str] = []
    all_tables: list[list[list[Any]]] = []

    with pdfplumber.open(io.BytesIO(file_content)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            full_text_parts.append(t)
        all_tables = _extract_all_tables(pdf)

    text = "\n".join(full_text_parts)
    driver = _find_driver_name(text)

    pm = re.search(r"Percent\s+(\d+)\s*%", text, re.I)
    percent = int(pm.group(1)) if pm else None
    if percent is None:
        warnings.append("PDF dan 'Percent XX%' topilmadi.")

    ws, we, wraw = _parse_work_period(text)
    if not ws:
        warnings.append(
            "Ish davri (Work Period) sanalari topilmadi — sheet tanlashda oxirgi haftalar qidiriladi."
        )

    trips: list[dict[str, Any]] = []
    trips_from_table = False
    for tb in all_tables:
        trips.extend(_parse_trips_from_table_strict(tb))
    if not trips:
        for tb in all_tables:
            trips.extend(_parse_trips_from_table_loose(tb))
    if trips:
        trips_from_table = True
    if not trips:
        trips = _parse_trips_from_free_text(text)
    if not trips:
        trips = _parse_trips_whole_text_brute(text)

    trips = _dedupe_trips(trips)
    # Jadvaldan olingan triplar uchun matn bo'yicha "nearby $" override qatorlarni siljitishi mumkin.
    # Override faqat jadval ajratilmagan holatda ishlatiladi.
    if not trips_from_table:
        trips = _normalize_trip_rates_by_text(trips, text)
    trips = _repair_suspicious_trip_rates(trips, text)
    fuel_entries, fuel_total = _extract_fuel_transactions(text)
    toll_entries, toll_total = _extract_toll_transactions(text)

    if not trips:
        warnings.append(
            "Trips jadvalidan ma'lumot ajratilmadi (PDF tuzilishi boshqa formatda bo'lishi mumkin)."
        )

    if not driver:
        warnings.append("Haydovchi ismi avtomatik topilmadi; Excelda tekshiring.")

    anchor = _pick_sheet_anchor_date(ws, we)

    return {
        "driver_name": driver or "",
        "percent": percent,
        "work_period_start": ws,
        "work_period_end": we,
        "work_period_raw": wraw,
        "anchor_date": anchor,
        "trips": trips,
        "fuel_transactions": fuel_entries,
        "fuel_total_pay_amount": fuel_total,
        "toll_transactions": toll_entries,
        "toll_total_pay_amount": toll_total,
        "parse_warnings": warnings,
        "source": "heuristic",
    }


def parse_company_driver_settlement_pdf_ai(file_content: bytes) -> dict[str, Any]:
    """
    GPT + heuristik gibrid parser:
    - AI dan kerakli maydonlar olinadi
    - AI da bo'sh qolgan joylar heuristik parser bilan to'ldiriladi
    """
    base = parse_company_driver_settlement_pdf(file_content)
    warnings = list(base.get("parse_warnings") or [])

    with pdfplumber.open(io.BytesIO(file_content)) as pdf:
        page_texts = [(p.extract_text() or "") for p in pdf.pages]
    joined_text = "\n\n".join(page_texts)
    ai = _call_openai_settlement_parser(joined_text)
    if not ai:
        warnings.append("AI parse ishlamadi; standart parser natijasi ishlatildi.")
        base["parse_warnings"] = warnings
        return base

    ai_driver = str(ai.get("driver_name") or "").strip()
    ai_percent = ai.get("percent")
    ai_wp_raw = str(ai.get("work_period_raw") or "").strip() or None
    ai_ws = _date_from_iso(ai.get("work_period_start"))
    ai_we = _date_from_iso(ai.get("work_period_end"))
    ai_trips_raw = ai.get("trips") if isinstance(ai.get("trips"), list) else []
    ai_fuel_total = _clean_money(ai.get("fuel_total_pay_amount"))
    ai_toll_total = _clean_money(ai.get("toll_total_pay_amount"))

    ai_trips: list[dict[str, Any]] = []
    for t in ai_trips_raw:
        if not isinstance(t, dict):
            continue
        tid = str(t.get("trip_id") or "").strip()
        rate = _clean_money(t.get("rate_gross"))
        if tid and rate is not None and float(rate) >= 100:
            ai_trips.append({"trip_id": tid, "rate_gross": rate})
    ai_trips = _dedupe_trips(ai_trips)
    merged_trips = _merge_trip_lists(ai_trips, base.get("trips") or [])

    merged = {
        "driver_name": ai_driver or base.get("driver_name") or "",
        "percent": int(ai_percent) if isinstance(ai_percent, (int, float)) else base.get("percent"),
        "work_period_start": ai_ws or base.get("work_period_start"),
        "work_period_end": ai_we or base.get("work_period_end"),
        "work_period_raw": ai_wp_raw or base.get("work_period_raw"),
        "anchor_date": _pick_sheet_anchor_date(
            ai_ws or base.get("work_period_start"),
            ai_we or base.get("work_period_end"),
        ),
        "trips": merged_trips,
        "fuel_transactions": base.get("fuel_transactions") or [],
        "fuel_total_pay_amount": ai_fuel_total if ai_fuel_total is not None else base.get("fuel_total_pay_amount"),
        "toll_transactions": base.get("toll_transactions") or [],
        "toll_total_pay_amount": ai_toll_total if ai_toll_total is not None else base.get("toll_total_pay_amount"),
        "parse_warnings": warnings,
        "source": "ai+heuristic",
        "ai_debug": {
            "trip_count_ai": len(ai_trips),
            "trip_count_final": len(merged_trips),
            "work_period_start_ai": _to_iso(ai_ws),
            "work_period_end_ai": _to_iso(ai_we),
            "fuel_total_ai": ai_fuel_total,
            "toll_total_ai": ai_toll_total,
        },
    }
    if not ai_trips:
        warnings.append("AI trips ajratmadi; trips uchun standart parser ishlatildi.")
    merged["parse_warnings"] = warnings
    return merged
