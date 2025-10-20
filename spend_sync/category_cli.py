import argparse
import os

from .airtable_client import AirtableClient
from .category_kpi import update_category_monthly_counts, update_order_kpis
from .constants import DEFAULT_CATEGORY_KPI_TABLE, DEFAULT_MONTHLY_KPI_TABLE, DEFAULT_ORDERS_TABLE
from .date_windows import dubai_now, monthly_windows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update Airtable KPI tables from orders.")
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
        help=f"Airtable orders table name or ID (default: env or '{DEFAULT_ORDERS_TABLE}').",
    )
    parser.add_argument(
        "--kpi-table",
        default=os.getenv("AIRTABLE_KPI_TABLE_NAME", DEFAULT_MONTHLY_KPI_TABLE),
        help=f"KPI table name or ID (default: env or '{DEFAULT_MONTHLY_KPI_TABLE}').",
    )
    parser.add_argument(
        "--category-table",
        default=os.getenv("AIRTABLE_CATEGORY_KPI_TABLE_NAME", DEFAULT_CATEGORY_KPI_TABLE),
        help=f"Category KPI table name or ID (default: env or '{DEFAULT_CATEGORY_KPI_TABLE}').",
    )
    parser.add_argument(
        "--skip-orders",
        action="store_true",
        help="Skip updating KPI (orders) table; only refresh category counts.",
    )
    parser.add_argument(
        "--skip-category",
        action="store_true",
        help="Skip updating category counts; only refresh KPI (orders) table.",
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

    now = dubai_now()
    previous_start, previous_end, current_start, current_end = monthly_windows(now)
    windows = (previous_start, previous_end), (current_start, current_end)

    if not args.skip_orders:
        update_order_kpis(
            airtable,
            args.orders_table,
            args.kpi_table,
            windows[0],
            windows[1],
        )

    if not args.skip_category:
        update_category_monthly_counts(
            airtable,
            args.orders_table,
            args.category_table,
            windows[0],
            windows[1],
        )


if __name__ == "__main__":
    main()
