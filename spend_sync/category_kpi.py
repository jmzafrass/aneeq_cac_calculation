import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from .airtable_client import AirtableClient


def _parse_airtable_date(value: object) -> Optional[dt.date]:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # Airtable sends ISO strings, sometimes with a trailing 'Z'.
        if len(text) >= 10:
            try:
                return dt.date.fromisoformat(text[:10])
            except ValueError:
                pass
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return dt.datetime.strptime(text[:10], fmt).date()
            except ValueError:
                continue
    return None


def _extract_categories(value: object) -> List[str]:
    entries: Iterable[str]
    if isinstance(value, list):
        entries = [str(item) for item in value if str(item).strip()]
    elif isinstance(value, str):
        entries = [value]
    else:
        return []

    results = []
    for entry in entries:
        for part in entry.split(","):
            label = part.strip()
            if label:
                results.append(label)
    return list(dict.fromkeys(results))


@dataclass
class OrderBucket:
    orders_all: int = 0
    revenue_all: float = 0.0
    orders_subs: int = 0
    revenue_subs: float = 0.0
    existing: int = 0
    multiple_orders: int = 0


STATUS_EXPECTED = "captured"
TYPE_NEW_SUB = "New Sub"
TYPE_RENEWAL = "Sub Renewal"
DATE_FIELD = "created_date"
PRODUCT_LINK_FIELD = "Product"
TYPE_FIELD = "Type"
STATUS_FIELD = "status"

ORDERS_USE_SUBS_ONLY = False
REVENUE_USE_SUBS_ONLY = False
AOV_USE_SUBS_ONLY = False

TZ_OFFSET = dt.timedelta(hours=4)

DECIMALS_BY_METRIC: Dict[str, int] = {
    "Revenue (aed)": 0,
    "Nbr Order": 0,
    "AOV (aed)": 0,
    "Existing": 0,
    "New orders": 0,
    "Multiple orders": 0,
}


def _normalize_expected(text: str) -> str:
    return text.strip().lower()


_STATUS_EXPECTED_NORM = _normalize_expected(STATUS_EXPECTED)
_TYPE_NEW_SUB_NORM = _normalize_expected(TYPE_NEW_SUB)
_TYPE_RENEWAL_NORM = _normalize_expected(TYPE_RENEWAL)


def _normalized(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        name = value.get("name")
        if isinstance(name, str):
            return name.strip().lower()
    if isinstance(value, list) and value:
        return _normalized(value[0])
    return str(value).strip().lower()


def _to_number(value: object) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        text = text.replace(",", "")
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _parse_airtable_datetime(value: object) -> Optional[dt.datetime]:
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time())
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        iso_candidate = text
        if iso_candidate.endswith("Z"):
            iso_candidate = iso_candidate[:-1] + "+00:00"
        try:
            return dt.datetime.fromisoformat(iso_candidate)
        except ValueError:
            pass
        patterns = (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
            "%d/%m/%Y %I:%M%p",
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y",
            "%m/%d/%Y %I:%M%p",
            "%m/%d/%Y %H:%M",
            "%m/%d/%Y",
        )
        for fmt in patterns:
            try:
                return dt.datetime.strptime(text, fmt)
            except ValueError:
                continue
    return None


def _to_dubai_date(value: object) -> Optional[dt.date]:
    dt_value = _parse_airtable_datetime(value)
    if not dt_value:
        return None
    if dt_value.tzinfo:
        dt_utc = dt_value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    else:
        dt_utc = dt_value
    return (dt_utc + TZ_OFFSET).date()


def _format_number(value: Optional[float], decimals: int = 0) -> str:
    if value is None:
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if decimals <= 0:
        return f"{int(round(number)):,d}"
    formatted = f"{number:,.{decimals}f}"
    return formatted


def _resolve_field(fields: Dict[str, object], preferred: str, *fallbacks: str) -> object:
    for key in (preferred, *fallbacks):
        if key in fields:
            return fields[key]
    return None


def update_order_kpis(
    airtable: AirtableClient,
    transactions_table: str,
    kpi_table: str,
    previous_window: Tuple[dt.date, dt.date],
    current_window: Tuple[dt.date, dt.date],
) -> None:
    previous_start, previous_end = previous_window
    current_start, current_end = current_window

    month_previous_label = previous_start.strftime("%B")
    month_current_label = current_start.strftime("%B")

    buckets = {"previous": OrderBucket(), "current": OrderBucket()}

    for record in airtable.iter_records(transactions_table):
        fields = record.get("fields", {})

        status_value = _resolve_field(fields, STATUS_FIELD, "Status")
        if _normalized(status_value) != _STATUS_EXPECTED_NORM:
            continue

        date_value = _resolve_field(fields, DATE_FIELD, "createdDate", "Created Date", "Order Date", "date", "Date")
        order_date = _to_dubai_date(date_value)
        if not order_date:
            continue

        if order_date < previous_start or order_date > current_end:
            continue

        bucket_key: Optional[str] = None
        if previous_start <= order_date <= previous_end:
            bucket_key = "previous"
        if current_start <= order_date <= current_end:
            # If a record somehow fits both windows we attribute it to current as well.
            if bucket_key is None:
                bucket_key = "current"
            else:
                # Duplicate into both buckets.
                for key in ("previous", "current"):
                    _accumulate_order(fields, buckets[key])
                continue

        if not bucket_key:
            continue

        _accumulate_order(fields, buckets[bucket_key])

    current_metrics = _compute_metrics(buckets["current"])
    previous_metrics = _compute_metrics(buckets["previous"])

    updates: List[Dict[str, object]] = []
    for record in airtable.iter_records(kpi_table):
        fields = record.get("fields", {})
        metric_name = fields.get("Metric")
        if not isinstance(metric_name, str):
            continue
        metric_key = metric_name.strip()
        if metric_key not in current_metrics:
            continue
        decimals = DECIMALS_BY_METRIC.get(metric_key, 0)
        updates.append(
            {
                "id": record["id"],
                "fields": {
                    month_previous_label: _format_number(previous_metrics[metric_key], decimals),
                    month_current_label: _format_number(current_metrics[metric_key], decimals),
                },
            }
        )

    if updates:
        airtable.update_records(kpi_table, updates)


def _accumulate_order(fields: Dict[str, object], bucket: OrderBucket) -> None:
    bucket.orders_all += 1

    amount_value = _resolve_field(fields, "amount", "Amount")
    amount = _to_number(amount_value)
    if amount is not None:
        bucket.revenue_all += amount

    type_value = _resolve_field(fields, TYPE_FIELD)
    type_normalized = _normalized(type_value)
    is_sub_type = type_normalized in {_TYPE_NEW_SUB_NORM, _TYPE_RENEWAL_NORM}
    if is_sub_type:
        bucket.orders_subs += 1
        if amount is not None:
            bucket.revenue_subs += amount
        if type_normalized == _TYPE_RENEWAL_NORM:
            bucket.existing += 1

    product_links = _resolve_field(fields, PRODUCT_LINK_FIELD)
    if isinstance(product_links, list):
        count = len([item for item in product_links if item is not None])
        if count > 1:
            bucket.multiple_orders += 1


def _compute_metrics(bucket: OrderBucket) -> Dict[str, float]:
    orders = bucket.orders_subs if ORDERS_USE_SUBS_ONLY else bucket.orders_all
    revenue = bucket.revenue_subs if REVENUE_USE_SUBS_ONLY else bucket.revenue_all

    aov_orders = bucket.orders_subs if AOV_USE_SUBS_ONLY else bucket.orders_all
    aov_revenue = bucket.revenue_subs if AOV_USE_SUBS_ONLY else bucket.revenue_all
    aov = round(aov_revenue / aov_orders) if aov_orders else 0

    existing = bucket.existing
    new_orders = max(orders - existing, 0)

    return {
        "Revenue (aed)": revenue,
        "Nbr Order": orders,
        "AOV (aed)": aov,
        "Existing": existing,
        "New orders": new_orders,
        "Multiple orders": bucket.multiple_orders,
    }


def update_category_monthly_counts(
    airtable: AirtableClient,
    orders_table: str,
    category_table: str,
    previous_window: Tuple[dt.date, dt.date],
    current_window: Tuple[dt.date, dt.date],
) -> None:
    previous_start, previous_end = previous_window
    current_start, current_end = current_window

    month_previous_label = previous_start.strftime("%B")
    month_current_label = current_start.strftime("%B")

    counts: Dict[str, Dict[str, int]] = defaultdict(lambda: {month_previous_label: 0, month_current_label: 0})

    order_records = airtable.iter_records(orders_table)

    range_start = previous_start
    range_end = current_end

    # Airtable field names might differ slightly between views, so try common fallbacks.
    def resolve_field(fields: Dict[str, object], preferred: str, fallbacks: Iterable[str]) -> object:
        candidates = [preferred, *fallbacks]
        for candidate in candidates:
            if candidate in fields:
                return fields[candidate]
        return None

    for record in order_records:
        fields = record.get("fields", {})
        order_date_value = resolve_field(fields, "Order Date", ("date", "Date"))
        order_date = _parse_airtable_date(order_date_value)
        if not order_date:
            continue
        if order_date < range_start or order_date > range_end:
            continue

        category_value = resolve_field(fields, "Category (from Product)", ("Category",))
        categories = _extract_categories(category_value)
        if not categories:
            continue

        in_previous = previous_start <= order_date <= previous_end
        in_current = current_start <= order_date <= current_end
        if not in_previous and not in_current:
            continue

        for category in categories:
            bucket = counts[category]
            if in_previous:
                bucket[month_previous_label] += 1
            if in_current:
                bucket[month_current_label] += 1

    category_field_name = "Category"
    existing_records: Dict[str, str] = {}
    for record in airtable.iter_records(category_table):
        fields = record.get("fields", {})
        name = fields.get(category_field_name)
        if isinstance(name, list):
            name = name[0] if name else None
        if not isinstance(name, str):
            continue
        normalized = name.strip()
        if not normalized:
            continue
        existing_records[normalized] = record["id"]

    all_categories = set(counts.keys()) | set(existing_records.keys())

    def totals(category: str) -> Tuple[int, int]:
        bucket = counts.get(category)
        if not bucket:
            return 0, 0
        return bucket.get(month_current_label, 0), bucket.get(month_previous_label, 0)

    sorted_categories = sorted(
        all_categories,
        key=lambda cat: (-totals(cat)[0], -totals(cat)[1], cat.lower()),
    )

    rank_by_category = {cat: index + 1 for index, cat in enumerate(sorted_categories)}

    updates: List[Dict[str, object]] = []
    creates: List[Dict[str, object]] = []

    for category in sorted_categories:
        current_total, previous_total = totals(category)
        fields_payload: Dict[str, object] = {
            month_previous_label: previous_total,
            month_current_label: current_total,
        }

        record_id = existing_records.get(category)
        if record_id:
            updates.append({"id": record_id, "fields": fields_payload})
        else:
            create_fields = {**fields_payload, category_field_name: category}
            creates.append({"fields": create_fields})

    if updates:
        airtable.update_records(category_table, updates)
    if creates:
        airtable.create_records(category_table, creates)
