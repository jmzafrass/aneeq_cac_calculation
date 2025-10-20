import datetime as dt
from collections import defaultdict
from typing import Dict, List, Tuple

from .airtable_client import AirtableClient
from .config import RuntimeConfig
from .date_windows import daily_windows, dubai_now, monthly_windows, required_start_date
from .category_kpi import update_category_monthly_counts, update_order_kpis
from .kpi import update_daily_cac, update_monthly_cac
from .sources import SpendRow, fetch_google_sheet_daily, fetch_meta_daily

DEFAULT_MONTHLY_KPI_TABLE = "KPI"
DEFAULT_ORDERS_TABLE = "Mamo Transactions"
DEFAULT_CATEGORY_KPI_TABLE = "KPI Category Monthly"


def fetch_spend(config: RuntimeConfig, start_date: dt.date, end_date: dt.date) -> List[SpendRow]:
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    rows: List[SpendRow] = []

    for account_id in config.meta_account_ids:
        rows.extend(
            fetch_meta_daily(
                access_token=config.meta_access_token,
                account_id=account_id,
                start_date=start_str,
                end_date=end_str,
            )
        )

    if config.google_sheet_url and config.google_account_id:
        rows.extend(
            fetch_google_sheet_daily(
                sheet_url=config.google_sheet_url,
                account_id=config.google_account_id,
                start_date=start_str,
                end_date=end_str,
            )
        )

    rows.sort(key=lambda r: (r.account_id, r.date))
    return rows


def to_airtable_payload(rows: List[SpendRow], pulled_at: str) -> List[Dict]:
    payload = []
    for row in rows:
        identifier = f"{dt.datetime.strptime(row.date, '%Y-%m-%d').strftime('%m/%d/%Y')} - {row.account_id}"
        payload.append(
            {
                "fields": {
                    "id": identifier,
                    "date": row.date,
                    "account_id": row.account_id,
                    "currency": row.currency,
                    "spend": row.amount,
                    "pulled_at": pulled_at,
                    "platform": row.platform,
                }
            }
        )
    return payload


def aggregate_by_date(rows: List[SpendRow]) -> Dict[str, int]:
    totals = defaultdict(int)
    for row in rows:
        totals[row.date] += row.amount
    return totals


def compute_required_start(now: dt.datetime) -> dt.date:
    return required_start_date(now)


def run_pipeline(config: RuntimeConfig) -> None:
    now = dubai_now()
    required_start = compute_required_start(now)
    kpi_rows = fetch_spend(config, required_start, config.fact_end_date)
    fact_start_iso = config.fact_start_date.isoformat()
    fact_rows = [row for row in kpi_rows if row.date >= fact_start_iso]
    rows = kpi_rows
    pulled_at = now.replace(microsecond=0).isoformat() + "Z"
    if not config.skip_airtable:
        airtable = AirtableClient(config.airtable_api_key, config.airtable_base_id)
        payload = to_airtable_payload(fact_rows, pulled_at)
        table_identifier = config.airtable_table_id or config.airtable_table_name or ""
        airtable.upsert_by_id(table_identifier, payload)

        category_table = (
            config.category_kpi_table_id
            or config.category_kpi_table_name
            or DEFAULT_CATEGORY_KPI_TABLE
        )
        orders_table = (
            config.orders_table_id
            or config.orders_table_name
            or config.airtable_table_id
            or config.airtable_table_name
            or DEFAULT_ORDERS_TABLE
        )

        previous_start, previous_end, current_start, current_end = monthly_windows(now)
        if config.monthly_kpi_table_id or config.monthly_kpi_table_name:
            table = config.monthly_kpi_table_id or config.monthly_kpi_table_name or DEFAULT_MONTHLY_KPI_TABLE
        else:
            table = DEFAULT_MONTHLY_KPI_TABLE

        if table:
            if orders_table:
                update_order_kpis(
                    airtable,
                    orders_table,
                    table,
                    (previous_start, previous_end),
                    (current_start, current_end),
                )
            update_monthly_cac(
                airtable,
                table,
                kpi_rows,
                (previous_start, previous_end),
                (current_start, current_end),
            )

        if config.daily_kpi_table_id or config.daily_kpi_table_name:
            today, previous_day = daily_windows(now)
            table = config.daily_kpi_table_id or config.daily_kpi_table_name or ""
            spend_by_date = aggregate_by_date(kpi_rows)
            update_daily_cac(
                airtable,
                table,
                spend_by_date,
                today,
                previous_day,
            )

        if category_table and orders_table:
            update_category_monthly_counts(
                airtable,
                orders_table,
                category_table,
                (previous_start, previous_end),
                (current_start, current_end),
            )

    if config.csv_path:
        from pathlib import Path
        import csv

        path = Path(config.csv_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "date", "account_id", "currency", "spend", "pulled_at", "platform"])
            for row in rows:
                identifier = f"{dt.datetime.strptime(row.date, '%Y-%m-%d').strftime('%m/%d/%Y')} - {row.account_id}"
                writer.writerow([identifier, row.date, row.account_id, row.currency, row.amount, pulled_at, row.platform])
