import datetime as dt
from typing import Dict, Iterable, List, Tuple

from .airtable_client import AirtableClient, AirtableError
from .sources import SpendRow


def format_with_commas(value: float) -> str:
    return f"{int(round(value)):,d}"


def parse_numeric(value) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if not value:
        return 0.0
    return float(str(value).replace(",", ""))


def sum_spend_for_month(rows: Iterable[SpendRow], year: int, month: int, day_limit: int) -> int:
    total = 0
    for row in rows:
        date = dt.datetime.strptime(row.date, "%Y-%m-%d").date()
        if date.year == year and date.month == month and date.day <= day_limit and date <= dt.date.today():
            total += row.amount
    return total


def update_monthly_cac(
    airtable: AirtableClient,
    table_id: str,
    spend_rows: List[SpendRow],
    previous_window: Tuple[dt.date, dt.date],
    current_window: Tuple[dt.date, dt.date],
) -> None:
    previous_start, previous_end = previous_window
    current_start, current_end = current_window

    prev_sum = sum_spend_for_month(spend_rows, previous_start.year, previous_start.month, previous_end.day)
    cur_sum = sum_spend_for_month(spend_rows, current_start.year, current_start.month, current_end.day)

    if prev_sum == 0 and cur_sum == 0:
        return

    new_orders_record = airtable.get_single_record(table_id, "New orders")
    fields = new_orders_record.get("fields", {})
    prev_orders = parse_numeric(fields.get(previous_start.strftime("%B")))
    cur_orders = parse_numeric(fields.get(current_start.strftime("%B")))

    def cac(spend: int, orders: float) -> str:
        if orders <= 0:
            return ""
        return format_with_commas(spend / orders)

    cac_prev = cac(prev_sum, prev_orders)
    cac_cur = cac(cur_sum, cur_orders)

    cac_record = airtable.get_single_record(table_id, "CAC Converted (aed)")
    airtable.update_single_record(
        table_id,
        cac_record["id"],
        {
            previous_start.strftime("%B"): cac_prev,
            current_start.strftime("%B"): cac_cur,
        },
    )


def update_daily_cac(
    airtable: AirtableClient,
    table_id: str,
    spend_by_date: Dict[str, int],
    today: dt.date,
    previous_date: dt.date,
) -> None:
    spend_today = spend_by_date.get(today.isoformat(), 0)
    spend_prev = spend_by_date.get(previous_date.isoformat(), 0)
    if spend_today == 0 and spend_prev == 0:
        return

    new_orders_record = airtable.get_single_record(table_id, "New orders")
    fields = new_orders_record.get("fields", {})

    today_orders = parse_numeric(fields.get(today.strftime("%B")))
    previous_orders = parse_numeric(fields.get(previous_date.strftime("%B")))

    def cac(spend: int, orders: float) -> str:
        if orders <= 0:
            return ""
        return format_with_commas(spend / orders)

    cac_today = cac(spend_today, today_orders)
    cac_prev = cac(spend_prev, previous_orders)

    cac_record = airtable.get_single_record(table_id, "CAC Converted (aed)")
    airtable.update_single_record(
        table_id,
        cac_record["id"],
        {
            today.strftime("%B"): cac_today,
            previous_date.strftime("%B"): cac_prev,
        },
    )
