import argparse
import sys

from .config import add_common_arguments, build_config
from .date_windows import dubai_now, required_start_date
from .pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync ad spend data into Airtable.")
    add_common_arguments(parser)
    return parser


def main(argv=None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    now = dubai_now()
    required_start = required_start_date(now)
    config = build_config(args, required_start)
    run_pipeline(config)


if __name__ == "__main__":
    main()
