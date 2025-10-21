import datetime as dt
from collections import defaultdict
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


def update_category_monthly_counts(
    airtable: AirtableClient,
    orders_table: str,
    category_table: str,
    previous_window: Tuple[dt.date, dt.date],
    current_window: Tuple[dt.date, dt.date],
) -> Dict[str, object]:
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
        order_date_value = resolve_field(
            fields,
            "Order Date",
            (
                "date",
                "Date",
                "created_date",
                "Created Date",
                "createdDate",
            ),
        )
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

    return {
        "updates": len(updates),
        "creates": len(creates),
        "categories": sorted_categories,
    }
