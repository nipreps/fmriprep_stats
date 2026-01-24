#!/usr/bin/env python
"""Export MongoDB events into daily parquet files.

Deterministic export windows can be specified with ``--start-date`` and
``--end-date``. Dates are interpreted in the selected timezone (default: UTC)
to define day boundaries. When both ``--start-date`` and ``--end-date`` are
provided, they take precedence and ``--num-days`` is ignored.

The latest complete day is determined by looking up the maximum ``dateCreated``
value across the requested events, truncating it to a date, and checking whether
that timestamp falls within the current day (in the selected timezone, default
UTC). If the max timestamp is within the current day, the latest complete day
is assumed to be the previous day.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pymongo import MongoClient
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.api import ISSUES

DEFAULT_BATCH_SIZE = 1000


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


def _normalize_timestamp(value: datetime, tz: ZoneInfo) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(tz)


def _get_collection_edge(collection, sort_direction: int) -> datetime | None:
    doc = collection.find_one(sort=[("dateCreated", sort_direction)])
    if not doc:
        return None
    return doc.get("dateCreated")


def _get_date_bounds(db, events: Iterable[str], tz: ZoneInfo) -> tuple[date, datetime]:
    min_candidates = []
    max_candidates = []
    for event in events:
        collection = db[event]
        min_ts = _get_collection_edge(collection, 1)
        max_ts = _get_collection_edge(collection, -1)
        if min_ts is not None:
            min_candidates.append(_normalize_timestamp(min_ts, tz))
        if max_ts is not None:
            max_candidates.append(_normalize_timestamp(max_ts, tz))
    if not min_candidates or not max_candidates:
        raise RuntimeError("No records found in MongoDB for requested events.")
    earliest = min(min_candidates).date()
    latest = max(max_candidates)
    return earliest, latest


def _latest_complete_day(max_timestamp: datetime, tz: ZoneInfo) -> date:
    max_ts = _normalize_timestamp(max_timestamp, tz)
    today = datetime.now(tz).date()
    latest_day = max_ts.date()
    if latest_day == today:
        return latest_day - timedelta(days=1)
    return latest_day


def _resolve_day_range(
    *,
    earliest: date,
    latest_complete: date,
    start_date: date | None,
    end_date: date | None,
    num_days: int | None,
) -> list[date]:
    if start_date and end_date:
        if start_date > end_date:
            raise ValueError("start-date cannot be after end-date")
        start = start_date
        end = end_date
    elif end_date:
        end = end_date
        if num_days:
            start = end - timedelta(days=num_days - 1)
        else:
            start = earliest
    elif start_date:
        start = start_date
        if num_days:
            end = start + timedelta(days=num_days - 1)
        else:
            end = latest_complete
    else:
        end = latest_complete
        if num_days:
            start = end - timedelta(days=num_days - 1)
        else:
            start = earliest
    if start > end:
        return []
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _iter_batches(cursor, batch_size: int):
    batch = []
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _prepare_dataframe(records: list[dict]) -> pd.DataFrame:
    for record in records:
        if "_id" in record:
            record["_id"] = str(record["_id"])
    frame = pd.DataFrame(records)
    if "dateCreated" in frame.columns:
        frame["dateCreated"] = pd.to_datetime(frame["dateCreated"])
    return frame


def _write_parquet(collection, day: date, output_path: Path, tz: ZoneInfo) -> int:
    day_start = datetime.combine(day, time.min, tzinfo=tz).astimezone(timezone.utc)
    day_end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=tz).astimezone(
        timezone.utc
    )
    query = {
        "dateCreated": {
            "$gte": day_start.replace(tzinfo=None),
            "$lt": day_end.replace(tzinfo=None),
        }
    }
    cursor = collection.find(query, batch_size=DEFAULT_BATCH_SIZE)
    writer = None
    total = 0
    for batch in _iter_batches(cursor, DEFAULT_BATCH_SIZE):
        frame = _prepare_dataframe(batch)
        table = pa.Table.from_pandas(frame, preserve_index=False)
        if writer is None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(output_path, table.schema)
        writer.write_table(table)
        total += len(frame)
    if writer:
        writer.close()
    return total


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export daily parquet files from MongoDB")
    parser.add_argument(
        "--events",
        nargs="+",
        default=list(ISSUES.keys()),
        help="Events to export (default: all ISSUES keys)",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write parquet files",
    )
    parser.add_argument("--num-days", type=int, default=None, help="Number of days to export")
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default=None,
        help="Start date (YYYY-MM-DD). Used with end-date for deterministic windows.",
    )
    parser.add_argument(
        "--end-date",
        type=_parse_date,
        default=None,
        help="End date (YYYY-MM-DD). Used with start-date for deterministic windows.",
    )
    parser.add_argument(
        "--timezone",
        default="UTC",
        help="Timezone name for day boundaries (default: UTC)",
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

    db = client.fmriprep_stats
    events = args.events

    try:
        earliest, latest_ts = _get_date_bounds(db, events, tz)
    except Exception as exc:
        logging.error("Failed to determine date bounds: %s", exc)
        return 1

    latest_complete = _latest_complete_day(latest_ts, tz)
    days = _resolve_day_range(
        earliest=earliest,
        latest_complete=latest_complete,
        start_date=args.start_date,
        end_date=args.end_date,
        num_days=args.num_days,
    )

    if not days:
        logging.warning("No days resolved for export.")
        return 0

    output_dir = Path(args.output_dir)
    summary = {event: {"files": 0, "records": 0} for event in events}

    for event in events:
        collection = db[event]
        safe_event = _sanitize_event_name(event)
        for day in days:
            output_path = output_dir / f"{day:%Y%m%d}-{safe_event}.parquet"
            logging.info("Exporting %s for %s to %s", event, day.isoformat(), output_path)
            count = _write_parquet(collection, day, output_path, tz)
            if count:
                summary[event]["files"] += 1
                summary[event]["records"] += count
                logging.info("Wrote %s records for %s (%s)", count, event, day)
            else:
                logging.info("No records for %s on %s", event, day)
    client.close()

    logging.info("Export summary:")
    for event, stats in summary.items():
        logging.info("%s: %s files, %s records", event, stats["files"], stats["records"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
