import argparse
import datetime as dt
import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class RuntimeConfig:
    fact_start_date: dt.date
    fact_end_date: dt.date
    fact_rolling_days: int
    meta_account_ids: List[str]
    google_sheet_url: Optional[str]
    google_account_id: Optional[str]
    csv_path: str
    airtable_base_id: str
    airtable_table_id: str
    airtable_table_name: Optional[str]
    airtable_api_key: str
    monthly_kpi_table_id: Optional[str]
    monthly_kpi_table_name: Optional[str]
    daily_kpi_table_id: Optional[str]
    daily_kpi_table_name: Optional[str]
    meta_access_token: str
    skip_airtable: bool


def parse_account_ids(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    result = []
    for part in raw.split(","):
        normalized = part.strip()
        if not normalized:
            continue
        if normalized.startswith("act_"):
            normalized = normalized[4:]
        result.append(normalized)
    return result


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--start-date",
        default=os.getenv("START_DATE"),
        help="Inclusive start date (YYYY-MM-DD). Default: $START_DATE or computed from KPI windows.",
    )
    parser.add_argument(
        "--end-date",
        default=os.getenv("END_DATE"),
        help="Inclusive end date (YYYY-MM-DD). Default: $END_DATE or today.",
    )
    parser.add_argument(
        "--account-id",
        dest="meta_account_ids",
        action="append",
        default=None,
        help="Meta ad account ID without 'act_' prefix (repeatable or comma-separated). "
        "Default: $META_AD_ACCOUNT_IDS or $META_AD_ACCOUNT_ID.",
    )
    parser.add_argument(
        "--google-sheet-url",
        default=os.getenv("GOOGLE_SPEND_SHEET_URL"),
        help="Published CSV URL for Google Ads spend (default: $GOOGLE_SPEND_SHEET_URL).",
    )
    parser.add_argument(
        "--google-account-id",
        default=os.getenv("GOOGLE_ADS_ACCOUNT_ID"),
        help="Google Ads account/customer ID (default: $GOOGLE_ADS_ACCOUNT_ID).",
    )
    parser.add_argument(
        "--csv-path",
        default=os.getenv("CSV_PATH", "data/meta_spend_daily.csv"),
        help="Destination CSV path. Default: data/meta_spend_daily.csv or $CSV_PATH.",
    )
    parser.add_argument(
        "--rolling-days",
        type=int,
        default=int(os.getenv("ROLLING_DAYS", "2") or 2),
        help="Number of recent days to refresh in the fact table when start date is not provided (default 2).",
    )
    parser.add_argument(
        "--skip-airtable",
        action="store_true",
        help="Skip writing to Airtable.",
    )


def build_config(args: argparse.Namespace, required_start: dt.date) -> RuntimeConfig:
    today = dt.date.today()
    if args.end_date:
        end_date = dt.datetime.strptime(args.end_date, "%Y-%m-%d").date()
    else:
        end_date = today

    rolling_days = args.rolling_days if hasattr(args, "rolling_days") else None
    if rolling_days is None:
        rolling_days = int(os.getenv("ROLLING_DAYS", "2") or 2)
    if rolling_days <= 0:
        rolling_days = 2

    if args.start_date:
        start_date = dt.datetime.strptime(args.start_date, "%Y-%m-%d").date()
    else:
        start_date = end_date - dt.timedelta(days=rolling_days - 1)

    if start_date > end_date:
        raise ValueError("start-date must be before or equal to end-date.")

    meta_ids: List[str] = []
    if args.meta_account_ids:
        for raw in args.meta_account_ids:
            meta_ids.extend(parse_account_ids(raw))
    else:
        env_ids = os.getenv("META_AD_ACCOUNT_IDS")
        if env_ids:
            meta_ids.extend(parse_account_ids(env_ids))
        else:
            fallback = parse_account_ids(os.getenv("META_AD_ACCOUNT_ID"))
            meta_ids.extend(fallback)

    if not meta_ids:
        raise ValueError("No Meta account IDs provided.")

    meta_token = os.getenv("META_ACCESS_TOKEN")
    if not meta_token:
        raise ValueError("META_ACCESS_TOKEN is required.")

    airtable_key = os.getenv("AIRTABLE_API_KEY")
    if not airtable_key:
        raise ValueError("AIRTABLE_API_KEY is required.")

    airtable_base_id = os.getenv("AIRTABLE_BASE_ID")
    if not airtable_base_id:
        raise ValueError("AIRTABLE_BASE_ID is required.")

    airtable_table_id = os.getenv("AIRTABLE_TABLE_ID", "")
    airtable_table_name = os.getenv("AIRTABLE_TABLE_NAME")
    if not airtable_table_id and not airtable_table_name:
        raise ValueError("Provide AIRTABLE_TABLE_ID or AIRTABLE_TABLE_NAME.")

    return RuntimeConfig(
        fact_start_date=start_date,
        fact_end_date=end_date,
        fact_rolling_days=rolling_days,
        meta_account_ids=meta_ids,
        google_sheet_url=args.google_sheet_url,
        google_account_id=(args.google_account_id or "").strip() or None,
        csv_path=args.csv_path,
        airtable_base_id=airtable_base_id,
        airtable_table_id=airtable_table_id,
        airtable_table_name=airtable_table_name,
        airtable_api_key=airtable_key,
        monthly_kpi_table_id=os.getenv("AIRTABLE_KPI_TABLE_ID"),
        monthly_kpi_table_name=os.getenv("AIRTABLE_KPI_TABLE_NAME"),
        daily_kpi_table_id=os.getenv("AIRTABLE_KPI_DAILY_TABLE_ID"),
        daily_kpi_table_name=os.getenv("AIRTABLE_KPI_DAILY_TABLE_NAME"),
        meta_access_token=meta_token,
        skip_airtable=args.skip_airtable,
    )
