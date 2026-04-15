"""
Company Driver settlement PDF dan haydovchi, foiz, ish davri va trip/reytinglarini ajratish.
Turli PDF generatorlar (TurboTax, payroll va h.k.) uchun bir nechta ajratish strategiyasi.
"""
from __future__ import annotations

import io
import re
from datetime import date
from typing import Any

import pdfplumber

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


def _extract_gross_rate_from_cell(cell: Any) -> float | None:
    """
    Rate (Gross) katagidan aynan gross summani oladi:
    - odatda birinchi qatordagi katta summa ($2,800.00)
    - pastdagi $/mi (masalan $2.473 /mi) ni hisobga olmaydi
    """
    if cell is None:
        return None
    raw = str(cell).replace("\r", "\n")
    # Avval qatorlar bo'yicha: 1-qatorda gross bo'lishi ehtimoli eng yuqori
    for line in [x.strip() for x in raw.split("\n") if x.strip()]:
        # /mi bo'lsa bu unit-rate; o'tkazib yuboramiz
        if "/mi" in line.lower():
            continue
        vals = re.findall(r"\$\s*[\d,]+\.\d{2}", line)
        for v in vals:
            m = _clean_money(v)
            if m is not None and m >= 50:
                return m
    # fallback: katak ichidagi barcha valyuta qiymatlaridan eng kattasi (gross odatda eng katta)
    vals = re.findall(r"\$\s*[\d,]+\.\d{2}", raw)
    nums = [(_clean_money(v) or 0.0) for v in vals]
    nums = [n for n in nums if n >= 50]
    if nums:
        return max(nums)
    return _clean_money(raw)


def _trip_id_from_cell(cell: Any) -> str | None:
    if cell is None:
        return None
    raw = str(cell).replace("\r", "\n")
    lines = [x.strip() for x in re.split(r"[\n]+", raw) if x.strip()]
    if not lines:
        parts = re.split(r"\s+", raw.strip())
        lines = [p for p in parts if p]

    # Rangni bevosita o'qib bo'lmagani uchun heuristika:
    # - M-prefiksli ID eng ustun
    # - 6+ xonali raqam (load id) 5 xonali trip raqamidan ustun
    # - 70xxx ko'rinishidagi 5 xonali trip raqamga penalti
    # - Katakning pastroqda turgan qiymati (odatda yashil) afzal
    candidates: list[tuple[int, str]] = []
    for li, line in enumerate(lines):
        # M-prefiksli ID
        for m in re.finditer(r"\b(M\d{5,})\b", line, re.I):
            tok = m.group(1).upper()
            score = 100 + li * 3
            candidates.append((score, tok))
        # Raqamli ID
        for m in re.finditer(r"\b(\d{5,8})\b", line):
            tok = m.group(1)
            score = 40 + li * 3
            if len(tok) >= 6:
                score += 20
            # Ko'p PDFlarda qizil trip no 70xxx bo'ladi
            if len(tok) == 5 and tok.startswith("70"):
                score -= 35
            candidates.append((score, tok))

    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]
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
            for m in re.finditer(r"\b(\d{6,8})\b", line):
                cand = m.group(1)
                if cand not in ("202020", "202120", "202220", "202320", "202420", "202520", "202620"):
                    tid = cand

        # Faqat gross summa: /mi bo'lgan unit-rate qiymatni olmaymiz
        rate = _extract_gross_rate_from_cell(line)

        if tid and rate is not None and rate >= 50:
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
        if rate is not None and rate >= 100:
            out.append({"trip_id": tid, "rate_gross": rate})
    return out


def _dedupe_trips(trips: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for t in trips:
        tid = t.get("trip_id")
        if not tid or tid in seen:
            continue
        seen.add(tid)
        out.append(t)
    return out


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
    for tb in all_tables:
        trips.extend(_parse_trips_from_table_strict(tb))
    if not trips:
        for tb in all_tables:
            trips.extend(_parse_trips_from_table_loose(tb))
    if not trips:
        trips = _parse_trips_from_free_text(text)
    if not trips:
        trips = _parse_trips_whole_text_brute(text)

    trips = _dedupe_trips(trips)

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
        "parse_warnings": warnings,
    }
