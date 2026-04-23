from __future__ import annotations

import io
import json
import re
import urllib.error
import urllib.request
from datetime import date
from typing import Any

import pdfplumber

import config


def _clean_money(val: Any) -> float | None:
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


def _date_from_iso(s: str | None) -> date | None:
    if not s:
        return None
    try:
        y, m, d = [int(x) for x in str(s).split("-")]
        return date(y, m, d)
    except Exception:
        return None


def _pick_anchor(ws: date | None, we: date | None) -> date | None:
    if ws and we:
        return ws + (we - ws) // 2
    return ws or we


def _match_trips_driver_header(line: str) -> tuple[str, str] | None:
    """
    Trips jadvalidagi driver sarlavha qatori.
    Formatlar: '3345(13) John Doe', '829 Esmatullah Salimi', 'T1881 Jurvoski Hawthorne'
    """
    s = (line or "").strip()
    if not s:
        return None
    patterns: list[tuple[re.Pattern[str], int, int]] = [
        # 3345(13) Name
        (re.compile(r"^\s*(\d{3,5}\(\d{1,3}\))\s+(.+)$"), 1, 2),
        # T1881 Name (harf + raqam, keyin ism)
        (re.compile(r"^\s*([A-Za-z]\d+)\s+([A-Za-z].+)$"), 1, 2),
        # 829 Name — 3–4 raqam, keyin ism (5+ raqamli load id bilan aralashmasligi uchun (?!\d))
        (re.compile(r"^\s*(\d{3,4}(?!\d))\s+([A-Za-z].+)$"), 1, 2),
    ]
    for pat, gi_label, gi_name in patterns:
        m = pat.match(s)
        if not m:
            continue
        label = m.group(gi_label).strip()
        raw_name = m.group(gi_name).strip()
        if not label or not raw_name:
            continue
        return (label, raw_name)
    return None


def _clean_driver_name(name: str | None, driver_label: str | None = None) -> str:
    raw = str(name or "").strip()
    label = str(driver_label or "").strip()
    if not raw and not label:
        return ""

    # Agar AI label bilan ismni birga qaytarsa: "3351(14) John Doe"
    if raw:
        m = re.match(r"^\s*\d{3,5}\(\d{1,3}\)\s+(.+)$", raw)
        if m:
            raw = m.group(1).strip()

    # Faqat raqamli label bo'lsa (masalan "3351(14)") - driver name emas.
    if raw and re.fullmatch(r"\d{3,5}\(\d{1,3}\)", raw):
        raw = ""

    # Boshida kelgan unit kodlarni olib tashlaymiz:
    # "829 Esmatullah Salimi" -> "Esmatullah Salimi"
    # "T1881 Jurvoski Hawthorne" -> "Jurvoski Hawthorne"
    if raw:
        raw = re.sub(r"^\s*[A-Za-z]?\d{2,6}\s+(.+)$", r"\1", raw).strip()

    # KPI/ulush satrlari driver emas.
    lowered = raw.lower()
    if (
        "percent" in lowered
        or "share" in lowered
        or re.search(r"\b\d{1,3}\s*%\b", lowered)
    ):
        return ""

    # Nomda kamida bitta harf bo'lishi shart.
    if raw and re.search(r"[A-Za-z]", raw):
        return re.sub(r"\s+", " ", raw).strip()
    return ""


def _is_trips_internal_trip_token(tok: str) -> bool:
    """Yuqori qator qizil trip raqami: odatda aynan 5 ta raqam (72173)."""
    t = (tok or "").strip()
    return bool(re.fullmatch(r"\d{5}", t))


def _trips_id_only_token(line: str) -> str | None:
    """Bitta load/ref tokeni bo'lgan qator (pastdagi yashil ID uchun buffer)."""
    s = (line or "").strip()
    if not s or len(s) > 48:
        return None
    if re.search(r"\$|mi\b|percent|share", s, re.I):
        return None
    m = re.match(r"^([0-9A-Za-z]{5,20})$", s, re.I)
    if not m:
        return None
    return m.group(1).strip()


def _pick_trips_green_load_id(stack: list[str]) -> str | None:
    """
    Birinchi ustunda yuqori qizil 5 xonali trip raqami, pastda yashil load ref.
    Ikkita bo'lsa — har doim pastdagi (oxirgi); bittasi bo'lsa va u faqat 5 raqam bo'lsa — tashlab ketamiz.
    """
    if not stack:
        return None
    if len(stack) >= 2:
        return stack[-1]
    only = stack[0]
    if _is_trips_internal_trip_token(only):
        return None
    return only


def _extract_trips_blocks_heuristic(text: str) -> list[dict[str, Any]]:
    """
    Faqat 'Trips' bo'limi: driver nomi (unit kodsiz) + har bir safar uchun
    pastdagi yashil load ref + Rate (Gross) birinchi $... (0.00 ham).
    Yuqori 5 xonali qizil trip raqamini load_id sifatida ishlatmaymiz.
    """
    lines = (text or "").replace("\r", "\n").split("\n")
    trip_header_re = re.compile(r"^\s*trips\b", re.I)
    money_re = re.compile(r"\$\s*([\d,]+\.\d{2})")
    # Bir qatorda: 72173 2299560 — ikkalasini buffer tartibida olamiz
    stacked_inline_re = re.compile(
        r"^\s*(\d{5})\s+([0-9A-Za-z]{5,20})\b",
        re.I,
    )
    section_break_re = re.compile(
        r"^\s*(P&L\s+Per\s+Truck|Fuel\s+Transaction|Toll\s+Transaction|"
        r"Recurring\s+Deductions|Settlement|Invoice|Owner\s+Operator)\b",
        re.I,
    )

    by_driver: dict[str, dict[str, Any]] = {}
    in_trips = False
    current_key: str | None = None
    recent_ids: list[str] = []

    def _ensure_driver(display_name: str, label: str) -> str:
        k = re.sub(r"\s+", " ", display_name.strip().lower())
        if k not in by_driver:
            by_driver[k] = {
                "driver_name": display_name.strip(),
                "driver_label": label,
                "trips": [],
            }
        return k

    def _flush_rate_line(line: str) -> None:
        nonlocal recent_ids
        if not current_key:
            recent_ids.clear()
            return
        low = line.lower()
        if ("percent" not in low and "share" not in low) or not money_re.search(line):
            return
        monies = [float(x.replace(",", "")) for x in money_re.findall(line)]
        if not monies:
            recent_ids.clear()
            return
        rate = monies[0]
        load_id = _pick_trips_green_load_id(recent_ids)
        recent_ids.clear()
        if not load_id:
            return
        trips_list = by_driver[current_key]["trips"]
        seen_row = {
            (re.sub(r"\s+", "", str(t.get("load_id") or "")).upper(), float(t.get("rate_gross") or 0))
            for t in trips_list
        }
        rid = re.sub(r"\s+", "", load_id).upper()
        row_key = (rid, rate)
        if row_key not in seen_row:
            trips_list.append({"load_id": load_id, "rate_gross": rate})

    for raw in lines:
        line = (raw or "").strip()
        if trip_header_re.match(line):
            in_trips = True
            current_key = None
            recent_ids.clear()
            continue
        if not in_trips:
            continue
        if section_break_re.match(line):
            in_trips = False
            current_key = None
            recent_ids.clear()
            continue

        hdr = _match_trips_driver_header(line)
        if hdr:
            recent_ids.clear()
            label_val, raw_name = hdr
            driver_name = _clean_driver_name(raw_name, label_val)
            if driver_name:
                current_key = _ensure_driver(driver_name, label_val)
            continue

        low = line.lower()
        if "mileage" in low and "rate" in low and "gross" in low:
            continue

        if current_key is None:
            continue

        if re.match(r"^\s*totals?\s*:", line, re.I):
            current_key = None
            recent_ids.clear()
            continue

        # Avvalo rate qatori (bir qatorda route + pul bo'lishi mumkin)
        if ("percent" in low or "share" in low) and money_re.search(line):
            _flush_rate_line(line)
            continue

        sm = stacked_inline_re.match(line)
        if sm:
            recent_ids.append(sm.group(1))
            recent_ids.append(sm.group(2))
            if len(recent_ids) > 6:
                recent_ids = recent_ids[-6:]
            continue

        tok = _trips_id_only_token(line)
        if tok:
            recent_ids.append(tok)
            if len(recent_ids) > 6:
                recent_ids = recent_ids[-6:]
            continue

    return list(by_driver.values())


def _extract_pnl_fuel_toll_heuristic(text: str) -> dict[str, dict[str, float]]:
    """
    Faqat P&L Per Truck bo'limidan driver -> fuel/toll qiymatlarini oladi.
    Kutilgan ustunlar tartibi: Gross Revenue, Earning, Fuel Cost, Toll Cost, ...
    """
    lines = (text or "").replace("\r", "\n").split("\n")
    in_pnl = False
    current_driver_key: str | None = None
    out: dict[str, dict[str, float]] = {}
    money_re = re.compile(r"\$\s*([\d,]+\.\d{2})")

    def _set_costs(driver_key: str | None, line: str) -> None:
        if not driver_key:
            return
        monies = [float(x.replace(",", "")) for x in money_re.findall(line or "")]
        # P&L satrida odatda kamida: Gross, Earning, Fuel, Toll
        if len(monies) < 4:
            return
        out[driver_key] = {"fuel": monies[2], "toll": monies[3]}

    for raw in lines:
        line = (raw or "").strip()
        low = line.lower()
        if "p&l per truck" in low:
            in_pnl = True
            current_driver_key = None
            continue
        if not in_pnl:
            continue
        # P&L bo'lim tugashi: Trips boshlanishi yoki keyingi katta bo'lim.
        if re.match(r"^\s*trips\b", line, re.I):
            break
        if re.match(
            r"^\s*(fuel\s+transaction|toll\s+transaction|settlement|depository|statement)\b",
            line,
            re.I,
        ):
            break
        if re.match(r"^\s*totals?\s*:", line, re.I):
            current_driver_key = None
            continue

        hdr = _match_trips_driver_header(line)
        if hdr:
            label_val, raw_name = hdr
            driver_name = _clean_driver_name(raw_name, label_val)
            if driver_name:
                current_driver_key = re.sub(r"\s+", " ", driver_name.strip().lower())
                # Ba'zi PDFlarda pul qiymatlari shu satrning o'zida bo'ladi.
                _set_costs(current_driver_key, line)
            continue

        _set_costs(current_driver_key, line)

    return out


def _extract_pnl_text_snippet(text: str, max_len: int = 14000) -> str:
    low = (text or "").lower()
    i = low.find("p&l per truck")
    if i < 0:
        return ""
    chunk = text[i : i + max_len * 2]
    lc = chunk.lower()
    cut = len(chunk)
    for stop in ("\ntrips", "\nfuel transaction", "\ntoll transaction", "\nsettlement"):
        j = lc.find(stop, 15)
        if j != -1:
            cut = min(cut, j)
    return chunk[:cut][:max_len]


def _call_openai_pnl_fuel_toll(snippet: str) -> dict[str, dict[str, float]] | None:
    """P&L Per Truck snippetidan driver -> fuel_cost, toll_cost (AI)."""
    api_key = (config.OPENAI_API_KEY or "").strip()
    model = (config.OPENAI_MODEL or "").strip() or "gpt-4.1-mini"
    sn = (snippet or "").strip()
    if not api_key or not sn:
        return None
    schema = {"rows": [{"driver_name": "string", "fuel_cost": "number", "toll_cost": "number"}]}
    prompt = (
        "You read ONLY the 'P&L Per Truck' table from this excerpt.\n"
        "Return one JSON object, no markdown.\n"
        "Rules:\n"
        "- One row per truck/driver line in the table.\n"
        "- driver_name: ONLY the person's name as shown (no truck number, no unit code in the name).\n"
        "- fuel_cost: value from column 'Fuel Cost' for that row.\n"
        "- toll_cost: value from column 'Toll Cost' for that row.\n"
        "- Skip the Totals row.\n"
        "- Use null if a value is missing.\n"
        f"JSON shape: {json.dumps(schema)}\n\n"
        "P&L excerpt:\n"
        f"{sn[:120000]}"
    )
    body = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [{"type": "input_text", "text": "You extract structured numbers from trucking P&L tables."}],
            },
            {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
        ],
        "temperature": 0,
    }
    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=75) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        return None
    output_text = payload.get("output_text")
    parsed: dict[str, Any] | None = None
    if isinstance(output_text, str) and output_text.strip():
        parsed = _extract_json_object(output_text)
    if not parsed:
        for item in payload.get("output") or []:
            for c in item.get("content") or []:
                txt = c.get("text")
                if isinstance(txt, str):
                    parsed = _extract_json_object(txt)
                    if parsed:
                        break
            if parsed:
                break
    if not parsed or not isinstance(parsed, dict):
        return None
    out: dict[str, dict[str, float]] = {}
    for row in parsed.get("rows") or []:
        if not isinstance(row, dict):
            continue
        nm = _clean_driver_name(row.get("driver_name"), None)
        if not nm:
            continue
        k = re.sub(r"\s+", " ", nm.lower())
        fuel = _clean_money(row.get("fuel_cost"))
        toll = _clean_money(row.get("toll_cost"))
        if fuel is None and toll is None:
            continue
        out[k] = {}
        if fuel is not None:
            out[k]["fuel"] = fuel
        if toll is not None:
            out[k]["toll"] = toll
    return out or None


def _call_openai_contractor_parser(text: str) -> dict[str, Any] | None:
    api_key = (config.OPENAI_API_KEY or "").strip()
    model = (config.OPENAI_MODEL or "").strip() or "gpt-4.1-mini"
    if not api_key or not text.strip():
        return None

    schema_hint = {
        "work_period_start": "YYYY-MM-DD|null",
        "work_period_end": "YYYY-MM-DD|null",
        "drivers": [
            {
                "driver_name": "string",
                "driver_label": "string|null",
                "trips": [{"load_id": "string", "rate_gross": "number"}],
                "fuel_total_pay_amount": "number|null",
                "toll_total_pay_amount": "number|null",
            }
        ],
    }
    prompt = (
        "Extract Contractor settlement data from this PDF text.\n"
        "Return only one JSON object, no markdown.\n"
        "Rules:\n"
        "- The file may contain MANY drivers in one document.\n"
        "- You MUST extract every driver block that appears in the document.\n"
        "- Driver header lines under Trips can look like:\n"
        "  '3345(13) Name Surname', '829 Esmatullah Salimi', 'T1881 Jurvoski Hawthorne'.\n"
        "- driver_name must be the person's name only (no unit code like 829 or T1881).\n"
        "- Do not stop after first driver; continue until document end.\n"
        "- For each driver section, extract all loads and their Rate (Gross).\n"
        "- Trips first column: NEVER use the top bold 5-digit red trip number alone (e.g. 72173).\n"
        "- ALWAYS use the second/bottom reference under it (e.g. 2299560, 22787225, 2056921B) as load_id.\n"
        "- If two IDs are stacked, load_id is the LOWER one only.\n"
        "- In Rate (Gross) cell, take only first/top amount, never '/mi'.\n"
        "- Do not use Net Amount as rate_gross.\n"
        "- For each driver, read Fuel Cost and Toll Cost from P&L Per Truck row.\n"
        "- Do NOT use Fuel Transaction/Toll Transaction sections for contractor totals when P&L row exists.\n"
        "- Keep trips unique per driver.\n"
        "- Use null if not found.\n"
        f"JSON shape: {json.dumps(schema_hint)}\n\n"
        "Document text:\n"
        f"{text[:160000]}"
    )
    body = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "You are a strict parser for trucking settlement PDFs.",
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
        with urllib.request.urlopen(req, timeout=95) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        return None

    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return _extract_json_object(output_text)
    for item in payload.get("output") or []:
        for c in item.get("content") or []:
            txt = c.get("text")
            if isinstance(txt, str):
                parsed = _extract_json_object(txt)
                if parsed:
                    return parsed
    return None


def _extract_driver_blocks(text: str) -> list[tuple[str, str]]:
    """
    Contractor PDF ichidan driver sarlavha bloklarini ajratadi.
    Misol: '3354(11) Michael McKethan', '829 Esmatullah Salimi', 'T1881 Jurvoski Hawthorne'
    """
    lines = (text or "").replace("\r", "\n").split("\n")
    starts: list[tuple[int, str]] = []
    for i, ln in enumerate(lines):
        s = (ln or "").strip()
        if _match_trips_driver_header(s):
            starts.append((i, s))
    if not starts:
        return []
    blocks: list[tuple[str, str]] = []
    for idx, (st, label) in enumerate(starts):
        en = starts[idx + 1][0] if idx + 1 < len(starts) else len(lines)
        block = "\n".join(lines[st:en]).strip()
        # Trips bo'lmagan juda qisqa bloklarni tashlab ketamiz.
        if len(block) < 60:
            continue
        blocks.append((label, block))
    return blocks


def _call_openai_contractor_block_parser(block_text: str, driver_label: str) -> dict[str, Any] | None:
    api_key = (config.OPENAI_API_KEY or "").strip()
    model = (config.OPENAI_MODEL or "").strip() or "gpt-4.1-mini"
    if not api_key or not (block_text or "").strip():
        return None
    schema_hint = {
        "driver_name": "string",
        "driver_label": "string|null",
        "trips": [{"load_id": "string", "rate_gross": "number"}],
        "fuel_total_pay_amount": "number|null",
        "toll_total_pay_amount": "number|null",
    }
    prompt = (
        "Extract ONE contractor driver block from settlement text.\n"
        "Return only one JSON object, no markdown.\n"
        "Rules:\n"
        "- Read only this block.\n"
        "- driver_label hint: " + driver_label + "\n"
        "- Extract all load_id and Rate (Gross) only from the Trips table for this driver.\n"
        "- Driver line may be 'UNIT Name' (e.g. 829 Name, T1881 Name, 3345(13) Name).\n"
        "- load_id = bottom/second reference in Trips first column, NOT the top 5-digit trip number.\n"
        "- Rate must be first/top amount in Rate (Gross), never '/mi', never Net Amount.\n"
        "- Read Fuel Cost and Toll Cost from this driver's P&L Per Truck row.\n"
        "- Do NOT use Fuel Transaction/Toll Transaction totals for this block unless P&L costs are missing.\n"
        f"JSON shape: {json.dumps(schema_hint)}\n\n"
        "Block text:\n"
        f"{block_text[:60000]}"
    )
    body = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [{"type": "input_text", "text": "You are a strict parser for trucking settlement PDFs."}],
            },
            {"role": "user", "content": [{"type": "input_text", "text": prompt}]},
        ],
        "temperature": 0,
    }
    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(body).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
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
    for item in payload.get("output") or []:
        for c in item.get("content") or []:
            txt = c.get("text")
            if isinstance(txt, str):
                parsed = _extract_json_object(txt)
                if parsed:
                    return parsed
    return None


def parse_contractor_settlement_pdf_ai(file_content: bytes) -> dict[str, Any]:
    warnings: list[str] = []
    with pdfplumber.open(io.BytesIO(file_content)) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)

    ai = _call_openai_contractor_parser(text)
    if not ai:
        return {
            "work_period_start": None,
            "work_period_end": None,
            "anchor_date": None,
            "drivers": [],
            "parse_warnings": ["AI contractor parse ishlamadi."],
            "source": "none",
        }

    ws = _date_from_iso(ai.get("work_period_start"))
    we = _date_from_iso(ai.get("work_period_end"))
    out_drivers: list[dict[str, Any]] = []
    for d in ai.get("drivers") or []:
        if not isinstance(d, dict):
            continue
        name = _clean_driver_name(d.get("driver_name"), d.get("driver_label"))
        if not name:
            continue
        trips = []
        seen = set()
        for t in d.get("trips") or []:
            if not isinstance(t, dict):
                continue
            lid = str(t.get("load_id") or "").strip()
            rate = _clean_money(t.get("rate_gross"))
            if not lid or rate is None:
                continue
            key = re.sub(r"\s+", "", lid).upper()
            if key in seen:
                continue
            seen.add(key)
            trips.append({"load_id": lid, "rate_gross": rate})
        out_drivers.append(
            {
                "driver_name": name,
                "driver_label": str(d.get("driver_label") or "").strip() or None,
                "trips": trips,
                "fuel_total_pay_amount": _clean_money(d.get("fuel_total_pay_amount")),
                "toll_total_pay_amount": _clean_money(d.get("toll_total_pay_amount")),
            }
        )

    block_drivers = []
    for label, block in _extract_driver_blocks(text):
        one = _call_openai_contractor_block_parser(block, label)
        if not one or not isinstance(one, dict):
            continue
        name = _clean_driver_name(one.get("driver_name"), one.get("driver_label"))
        if not name:
            hint = _match_trips_driver_header(str(label or "").strip())
            if hint:
                name = _clean_driver_name(hint[1], hint[0])
        if not name:
            continue
        trips = []
        seen = set()
        for t in one.get("trips") or []:
            if not isinstance(t, dict):
                continue
            lid = str(t.get("load_id") or "").strip()
            rate = _clean_money(t.get("rate_gross"))
            if not lid or rate is None:
                continue
            key = re.sub(r"\s+", "", lid).upper()
            if key in seen:
                continue
            seen.add(key)
            trips.append({"load_id": lid, "rate_gross": rate})
        if not trips:
            continue
        block_drivers.append(
            {
                "driver_name": name,
                "driver_label": str(one.get("driver_label") or "").strip() or label,
                "trips": trips,
                "fuel_total_pay_amount": _clean_money(one.get("fuel_total_pay_amount")),
                "toll_total_pay_amount": _clean_money(one.get("toll_total_pay_amount")),
            }
        )

    # Merge: AI umumiy natija + blok-parser natijasi (har doim)
    merged: dict[str, dict[str, Any]] = {}
    for d in out_drivers + block_drivers:
        key = re.sub(r"\s+", " ", str(d.get("driver_name") or "").strip().lower())
        if not key:
            continue
        if key not in merged:
            merged[key] = {
                "driver_name": d.get("driver_name"),
                "driver_label": d.get("driver_label"),
                "trips": [],
                "fuel_total_pay_amount": d.get("fuel_total_pay_amount"),
                "toll_total_pay_amount": d.get("toll_total_pay_amount"),
            }
        # Null bo'lsa qo'shimcha manbadan to'ldirish
        if merged[key]["fuel_total_pay_amount"] is None and d.get("fuel_total_pay_amount") is not None:
            merged[key]["fuel_total_pay_amount"] = d.get("fuel_total_pay_amount")
        if merged[key]["toll_total_pay_amount"] is None and d.get("toll_total_pay_amount") is not None:
            merged[key]["toll_total_pay_amount"] = d.get("toll_total_pay_amount")

        seen_ids = {
            re.sub(r"\s+", "", str(x.get("load_id") or "")).upper()
            for x in merged[key]["trips"]
        }
        for t in d.get("trips") or []:
            lid = str(t.get("load_id") or "").strip()
            rid = re.sub(r"\s+", "", lid).upper()
            if not lid or rid in seen_ids:
                continue
            merged[key]["trips"].append({"load_id": lid, "rate_gross": t.get("rate_gross")})
            seen_ids.add(rid)

    out_drivers = list(merged.values())

    # Trips sarlavhasi ostidan olingan heuristik natijani ustun qo'yamiz.
    # User talabi: load/rate faqat Trips bo'limidan olinishi kerak.
    trips_heur = _extract_trips_blocks_heuristic(text)
    if trips_heur:
        hmap = {
            re.sub(r"\s+", " ", str(x.get("driver_name") or "").strip().lower()): x
            for x in trips_heur
            if str(x.get("driver_name") or "").strip()
        }
        for d in out_drivers:
            k = re.sub(r"\s+", " ", str(d.get("driver_name") or "").strip().lower())
            if k in hmap and hmap[k].get("trips"):
                d["trips"] = hmap[k]["trips"]
        # Heuristikda topilib, AIda umuman yo'q driverlarni ham qo'shamiz
        existing = {
            re.sub(r"\s+", " ", str(d.get("driver_name") or "").strip().lower())
            for d in out_drivers
        }
        for h in trips_heur:
            hk = re.sub(r"\s+", " ", str(h.get("driver_name") or "").strip().lower())
            if hk in existing:
                continue
            out_drivers.append(
                {
                    "driver_name": h.get("driver_name"),
                    "driver_label": h.get("driver_label"),
                    "trips": h.get("trips") or [],
                    "fuel_total_pay_amount": None,
                    "toll_total_pay_amount": None,
                }
            )
        warnings.append(f"Trips-heuristic parser ishladi: {len(trips_heur)} ta driver tripsdan olindi.")

    # Fuel/Toll: avval P&L matndan heuristik, keyin shu bo'limga qaratilgan AI bilan ustun-ustun.
    pnl_costs = _extract_pnl_fuel_toll_heuristic(text)
    if pnl_costs:
        for d in out_drivers:
            k = re.sub(r"\s+", " ", str(d.get("driver_name") or "").strip().lower())
            item = pnl_costs.get(k)
            if not item:
                continue
            d["fuel_total_pay_amount"] = item.get("fuel")
            d["toll_total_pay_amount"] = item.get("toll")
        warnings.append(f"P&L (heuristic) ishladi: {len(pnl_costs)} ta driver Fuel/Toll.")
    pnl_snip = _extract_pnl_text_snippet(text)
    pnl_ai = _call_openai_pnl_fuel_toll(pnl_snip) if pnl_snip.strip() else None
    if pnl_ai:
        for d in out_drivers:
            k = re.sub(r"\s+", " ", str(d.get("driver_name") or "").strip().lower())
            item = pnl_ai.get(k)
            if not item:
                continue
            if "fuel" in item:
                d["fuel_total_pay_amount"] = item["fuel"]
            if "toll" in item:
                d["toll_total_pay_amount"] = item["toll"]
        warnings.append(f"P&L (AI) ishladi: {len(pnl_ai)} ta driver Fuel/Toll yangilandi.")

    if not out_drivers:
        warnings.append("PDFdan contractor driver bloklari ajratilmadi.")
    else:
        if text.lower().count("trips") >= 3 and len(out_drivers) < 3:
            warnings.append(
                f"PDFda Trips bloklari ko'p, lekin {len(out_drivers)} ta driver ajratildi. "
                "Driver nomlari formatini tekshirib qayta yuboring."
            )
        if block_drivers:
            warnings.append(
                f"Driver blok fallback ishladi: block parser {len(block_drivers)} ta driver topdi, jami {len(out_drivers)}."
            )

    return {
        "work_period_start": ws,
        "work_period_end": we,
        "anchor_date": _pick_anchor(ws, we),
        "drivers": out_drivers,
        "parse_warnings": warnings,
        "source": "ai",
    }

