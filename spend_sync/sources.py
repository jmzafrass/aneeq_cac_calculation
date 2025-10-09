import csv
import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from io import StringIO
from typing import Dict, Iterable, List, Optional, Tuple

import requests

META_API_VERSION = "v23.0"


class SourceError(RuntimeError):
    pass


@dataclass
class SpendRow:
    date: str
    account_id: str
    currency: str
    amount: int
    platform: str


def round_currency(value: str) -> Decimal:
    return Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def fetch_meta_daily(
    access_token: str,
    account_id: str,
    start_date: str,
    end_date: str,
    platform_label: str = "meta",
) -> List[SpendRow]:
    url = f"https://graph.facebook.com/{META_API_VERSION}/act_{account_id}/insights"
    params = {
        "fields": "spend,account_currency,date_start",
        "time_increment": 1,
        "level": "account",
        "time_range[since]": start_date,
        "time_range[until]": end_date,
        "access_token": access_token,
        "limit": 1000,
    }

    rows: List[SpendRow] = []
    next_url: Optional[str] = url
    first = True
    while next_url:
        resp = requests.get(next_url, params=params if first else None, timeout=120)
        first = False
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "30"))
            raise SourceError(f"Meta rate limited. Retry after {retry_after}s.")
        if resp.status_code >= 400:
            raise SourceError(f"Meta API error {resp.status_code}: {resp.text}")

        payload = resp.json()
        for record in payload.get("data", []):
            date = record.get("date_start")
            spend = record.get("spend", "0") or "0"
            currency = record.get("account_currency") or "AED"
            if not date:
                continue
            amount = int(round_currency(spend).quantize(Decimal("1")))
            rows.append(SpendRow(date=date, account_id=account_id, currency=currency, amount=amount, platform=platform_label))

        next_url = payload.get("paging", {}).get("next")

    rows.sort(key=lambda r: r.date)
    return rows


def _normalize_header(value: str) -> str:
    return "".join(ch for ch in (value or "").lower() if ch.isalnum())


def fetch_google_sheet_daily(
    sheet_url: str,
    account_id: str,
    start_date: str,
    end_date: str,
    platform_label: str = "google_ads",
    currency: str = "AED",
) -> List[SpendRow]:
    if not sheet_url:
        return []

    resp = requests.get(sheet_url, timeout=60)
    if resp.status_code >= 400:
        raise SourceError(f"Google sheet fetch error {resp.status_code}: {resp.text}")

    text = resp.text.splitlines()
    filtered: List[str] = []
    header_found = False
    for line in text:
        normalized = line.strip().lower()
        if not header_found:
            if normalized.startswith("date") and "," in line:
                header_found = True
                filtered.append(line)
        else:
            filtered.append(line)

    if not header_found:
        raise SourceError("Google sheet missing header row.")

    reader = csv.DictReader(StringIO("\n".join(filtered)))
    normalized_headers = {_normalize_header(col): col for col in reader.fieldnames or []}
    date_col = normalized_headers.get("date")
    cost_col = normalized_headers.get("costmicros") or normalized_headers.get("cost") or normalized_headers.get("amount")
    if not date_col or not cost_col:
        raise SourceError("Google sheet missing Date or Cost column.")

    totals: Dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    total_rows = 0
    for row in reader:
        total_rows += 1
        date_raw = (row.get(date_col) or "").strip()
        cost_raw = row.get(cost_col)
        if not date_raw:
            continue
        try:
            parsed = date_raw if "-" in date_raw else dt.datetime.strptime(date_raw, "%d/%m/%Y").strftime("%Y-%m-%d")
        except Exception:
            continue
        if parsed < start_date or parsed > end_date:
            continue
        cost_decimal = Decimal(str(cost_raw).replace(",", "")) if cost_raw not in (None, "") else Decimal("0")
        totals[parsed] += cost_decimal

    rows = [
        SpendRow(
            date=date,
            account_id=account_id,
            currency=currency,
            amount=int(amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP)),
            platform=platform_label,
        )
        for date, amount in sorted(totals.items())
    ]
    return rows
