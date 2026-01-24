#!/usr/bin/env python
"""Compare daily parquet row counts with MongoDB counts for a given event/day."""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq
from pymongo import MongoClient
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.api import ISSUES


def _sanitize_event_name(event_name: str) -> str:
    normalized = event_name.strip().lower()
    normalized = normalized.replace(os.sep, "_")
    if os.altsep:
        normalized = normalized.replace(os.altsep, "_")
    normalized = re.sub(r"[^a-z0-9_.-]+", "_", normalized)
    normalized = normalized.strip("_")
    return normalized or "event"


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        parsed = datetime.fromisoformat(value)
        return parsed.date()


def _day_bounds(day: date, tz: ZoneInfo) -> tuple[datetime, datetime]:
    day_start = datetime.combine(day, time.min, tzinfo=tz).astimezone(timezone.utc)
    day_end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=tz).astimezone(
        timezone.utc
    )
    return day_start, day_end


def _parquet_row_count(path: Path) -> int | None:
    if not path.exists():
        return None
    parquet_file = pq.ParquetFile(path)
    return parquet_file.metadata.num_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare MongoDB counts to parquet row counts for a day/event."
    )
    parser.add_argument(
        "--event",
        required=True,
        choices=sorted(ISSUES.keys()),
        help="Event name (must match Mongo collection name).",
    )
    parser.add_argument(
        "--day",
        required=True,
        type=_parse_date,
        help="Day to validate (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory containing parquet files.",
    )
    parser.add_argument(
        "--timezone",
        default="UTC",
        help="Timezone name for day boundaries (default: UTC).",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        tz = ZoneInfo(args.timezone)
    except Exception as exc:  # pragma: no cover - CLI guard
        logging.error("Invalid timezone '%s': %s", args.timezone, exc)
        return 2

    try:
        client = MongoClient()
        client.admin.command("ping")
    except Exception as exc:
        logging.error("Failed to connect to MongoDB: %s", exc)
        return 1

    day_start, day_end = _day_bounds(args.day, tz)
    query = {
        "dateCreated": {
            "$gte": day_start.replace(tzinfo=None),
            "$lt": day_end.replace(tzinfo=None),
        }
    }

    collection = client.fmriprep_stats[args.event]
    mongo_count = collection.count_documents(query)
    client.close()

    safe_event = _sanitize_event_name(args.event)
    parquet_path = Path(args.output_dir) / f"{args.day:%Y%m%d}-{safe_event}.parquet"
    parquet_count = _parquet_row_count(parquet_path)

    if parquet_count is None:
        logging.warning("Parquet file missing: %s", parquet_path)
        if mongo_count == 0:
            logging.info("Mongo count is zero; treating missing parquet file as empty.")
            return 0
        logging.error("Mongo count %s does not match missing parquet file.", mongo_count)
        return 1

    logging.info(
        "Mongo count: %s, parquet count: %s for %s on %s",
        mongo_count,
        parquet_count,
        args.event,
        args.day.isoformat(),
    )
    if mongo_count != parquet_count:
        logging.error("Count mismatch for %s on %s", args.event, args.day.isoformat())
        return 1

    logging.info("Parity check passed for %s on %s", args.event, args.day.isoformat())
    return 0


if __name__ == "__main__":
    sys.exit(main())
