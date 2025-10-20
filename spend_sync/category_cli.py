import argparse
import os

from .airtable_client import AirtableClient
from .category_kpi import update_category_monthly_counts
from .date_windows import dubai_now, monthly_windows

DEFAULT_ORDERS_TABLE = "Mamo Transactions"
DEFAULT_CATEGORY_TABLE = "KPI Category Monthly"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update Airtable category KPI table.")
    parser.add_argument(
        "--airtable-api-key",
        default=os.getenv("AIRTABLE_API_KEY"),
        help="Airtable API key (default: $AIRTABLE_API_KEY).",
    )
    parser.add_argument(
        "--airtable-base-id",
        default=os.getenv("AIRTABLE_BASE_ID"),
        help="Airtable base ID (default: $AIRTABLE_BASE_ID).",
    )
    parser.add_argument(
        "--orders-table",
        default=os.getenv("AIRTABLE_ORDERS_TABLE_NAME", DEFAULT_ORDERS_TABLE),
        help=f"Airtable orders table identifier (default: env or '{DEFAULT_ORDERS_TABLE}').",
    )
    parser.add_argument(
        "--category-table",
        default=os.getenv("AIRTABLE_CATEGORY_KPI_TABLE_NAME", DEFAULT_CATEGORY_TABLE),
        help=f"Category KPI table identifier (default: env or '{DEFAULT_CATEGORY_TABLE}').",
    )
    return parser


def main(argv=None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.airtable_api_key:
        raise SystemExit("AIRTABLE_API_KEY is required (pass via flag or env).")
    if not args.airtable_base_id:
        raise SystemExit("AIRTABLE_BASE_ID is required (pass via flag or env).")

    airtable = AirtableClient(args.airtable_api_key, args.airtable_base_id)

    previous_start, previous_end, current_start, current_end = monthly_windows(dubai_now())
    previous_window = (previous_start, previous_end)
    current_window = (current_start, current_end)

    update_category_monthly_counts(
        airtable,
        args.orders_table,
        args.category_table,
        previous_window,
        current_window,
    )
