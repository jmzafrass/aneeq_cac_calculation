import csv
import datetime as dt
import logging
import os
import re
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from io import StringIO
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

import requests

META_API_VERSION = "v23.0"
META_RATE_LIMIT_ENV_VAR = "META_MAX_CALLS_PER_MINUTE"
META_DEFAULT_CALLS_PER_MINUTE = 60
META_MAX_RETRIES = 5
META_BASE_BACKOFF_SECONDS = 15
META_MAX_BACKOFF_SECONDS = 300


class RateLimiter:
    """Simple token bucket style limiter that caps requests per time window."""

    def __init__(self, max_calls: int, per_seconds: float) -> None:
        self.max_calls = max(1, int(max_calls))
        self.per_seconds = max(0.1, float(per_seconds))
        self._lock = threading.Lock()
        self._timestamps: Deque[float] = deque()

    def wait(self) -> None:
        """Block until a request slot is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                while self._timestamps and now - self._timestamps[0] >= self.per_seconds:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_calls:
                    self._timestamps.append(now)
                    return
                sleep_for = self.per_seconds - (now - self._timestamps[0])
            time.sleep(max(0.05, min(sleep_for, 5.0)))


logger = logging.getLogger(__name__)
_meta_rate_limiter: Optional[RateLimiter] = None
_meta_rate_limiter_lock = threading.Lock()


def _get_meta_rate_limiter() -> RateLimiter:
    global _meta_rate_limiter
    if _meta_rate_limiter is not None:
        return _meta_rate_limiter
    with _meta_rate_limiter_lock:
        if _meta_rate_limiter is None:
            max_calls = os.getenv(META_RATE_LIMIT_ENV_VAR)
            try:
                calls = int(max_calls) if max_calls else META_DEFAULT_CALLS_PER_MINUTE
            except ValueError:
                logger.warning(
                    "Invalid %s value %r; falling back to default %s calls/minute.",
                    META_RATE_LIMIT_ENV_VAR,
                    max_calls,
                    META_DEFAULT_CALLS_PER_MINUTE,
                )
                calls = META_DEFAULT_CALLS_PER_MINUTE
            _meta_rate_limiter = RateLimiter(max_calls=calls, per_seconds=60.0)
    return _meta_rate_limiter


def _extract_retry_after_seconds(error: Dict[str, Any]) -> Optional[float]:
    data = error.get("error_data")
    if isinstance(data, dict):
        candidates = [
            data.get("retry_after"),
            data.get("estimated_time_to_regain_access"),
        ]
        for value in candidates:
            if value is None:
                continue
            try:
                return max(0.0, float(value))
            except (TypeError, ValueError):
                continue
    return None


def _meta_retry_delay(resp: requests.Response, attempt: int) -> Optional[float]:
    status = resp.status_code
    if status in {500, 502, 503, 504}:
        return min(META_BASE_BACKOFF_SECONDS * attempt, META_MAX_BACKOFF_SECONDS)
    if status == 429:
        header = resp.headers.get("Retry-After")
        if header:
            try:
                return max(float(header), 1.0)
            except (TypeError, ValueError):
                pass
        return min(META_BASE_BACKOFF_SECONDS * attempt, META_MAX_BACKOFF_SECONDS)
    if status == 403:
        error: Dict[str, Any] = {}
        try:
            payload = resp.json()
            if isinstance(payload, dict):
                error_obj = payload.get("error")
                if isinstance(error_obj, dict):
                    error = error_obj
        except ValueError:
            error = {}
        if not error:
            return None
        code = error.get("code")
        subcode = error.get("error_subcode")
        transient = bool(error.get("is_transient"))
        limiter_codes = {"4"}
        limiter_subcodes = {1504021, 1504022, 1504023}
        if transient or str(code) in limiter_codes or (isinstance(subcode, int) and subcode in limiter_subcodes):
            retry_after = _extract_retry_after_seconds(error)
            if retry_after is not None:
                return min(max(retry_after, 1.0), META_MAX_BACKOFF_SECONDS)
            return min(META_BASE_BACKOFF_SECONDS * attempt, META_MAX_BACKOFF_SECONDS)
    return None


def _request_meta_insights(url: str, params: Optional[Dict[str, Any]]) -> requests.Response:
    last_status = None
    last_text = None
    for attempt in range(1, META_MAX_RETRIES + 1):
        _get_meta_rate_limiter().wait()
        resp = requests.get(url, params=params, timeout=120)
        if resp.status_code < 400:
            return resp
        last_status = resp.status_code
        last_text = resp.text
        delay = _meta_retry_delay(resp, attempt)
        if delay is None:
            raise SourceError(f"Meta API error {resp.status_code}: {resp.text}")
        if attempt == META_MAX_RETRIES:
            raise SourceError(f"Meta API error {resp.status_code} after {META_MAX_RETRIES} retries: {resp.text}")
        logger.warning(
            "Meta API throttled request (status=%s). Sleeping %.1fs before retry %s/%s.",
            resp.status_code,
            delay,
            attempt,
            META_MAX_RETRIES,
        )
        time.sleep(delay)
        # For retries we must resend original query parameters.
    raise SourceError(f"Meta API error {last_status}: {last_text}")


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
        resp = _request_meta_insights(next_url, params if first else None)
        first = False

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


def _parse_cost_value(value: object) -> Optional[Decimal]:
    if value in (None, ""):
        return Decimal("0")
    text = str(value).strip()
    if not text:
        return Decimal("0")
    normalized = text.replace(",", "")
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = f"-{normalized[1:-1]}"
    normalized = re.sub(r"[^\d.\-]", "", normalized)
    if normalized in {"", "-", ".", "-.", ".-"}:
        return None
    try:
        return Decimal(normalized)
    except (InvalidOperation, ValueError):
        return None


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
        cost_decimal = _parse_cost_value(cost_raw)
        if cost_decimal is None:
            logger.warning("Skipping non-numeric cost value %r for date %s", cost_raw, parsed)
            continue
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
