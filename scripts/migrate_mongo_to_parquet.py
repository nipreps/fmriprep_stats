#!/usr/bin/env python3
"""Export MongoDB collections into Parquet partitions."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from pymongo import MongoClient

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from api import (  # noqa: E402  pylint: disable=wrong-import-position
    ISSUES,
    ManifestCache,
    _event_date,
    _normalize_event,
    _partition_path,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017", help="MongoDB URI")
    parser.add_argument("--db", default="fmriprep_stats", help="MongoDB database name")
    parser.add_argument(
        "--dataset-root",
        required=True,
        help="Target directory where Parquet files will be written",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of documents to stream from Mongo per batch",
    )
    parser.add_argument(
        "--start-date",
        type=lambda value: datetime.strptime(value, "%Y-%m-%d"),
        default=None,
        help="Only export events on/after this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=lambda value: datetime.strptime(value, "%Y-%m-%d"),
        default=None,
        help="Only export events before this date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--collections",
        nargs="+",
        default=None,
        help="Subset of collections to export (defaults to all supported issues)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return parser.parse_args()


def _date_filter(args: argparse.Namespace) -> Optional[Dict[str, str]]:
    if not args.start_date and not args.end_date:
        return None

    window: Dict[str, str] = {}
    if args.start_date:
        start_iso = args.start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        window["$gte"] = start_iso.strftime("%Y-%m-%dT%H:%M:%SZ")
    if args.end_date:
        end_iso = args.end_date.replace(hour=0, minute=0, second=0, microsecond=0)
        window["$lt"] = (end_iso + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return window


def _write_partition(
    dataset_root: Path,
    event_name: str,
    event_date,
    rows: List[Dict],
    manifest_cache: ManifestCache,
) -> None:
    if not rows:
        return

    target_path = _partition_path(dataset_root, event_name, event_date)
    relative_path = str(target_path.relative_to(dataset_root))
    if relative_path in manifest_cache.seen_paths:
        logging.info("Skipping %s – manifest already lists this partition", relative_path)
        return

    target_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    tmp_path = target_path.with_suffix(".tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(target_path)

    ids = [row.get("id") for row in rows if row.get("id")]
    manifest_cache.add(
        {
            "event": event_name,
            "date": event_date.isoformat(),
            "path": relative_path,
            "rows": len(rows),
            "min_id": min(ids) if ids else None,
            "max_id": max(ids) if ids else None,
        }
    )
    logging.info("Wrote %s (%d rows)", relative_path, len(rows))


def export_collection(
    client: MongoClient,
    db_name: str,
    event_name: str,
    dataset_root: Path,
    batch_size: int,
    date_filter: Optional[Dict[str, str]],
    manifest_cache: ManifestCache,
) -> Dict[str, int]:
    stats = {"read": 0, "written": 0, "skipped": 0}
    collection = client[db_name][event_name]

    query: Dict[str, Dict[str, str]] = {}
    if date_filter:
        query["dateCreated"] = date_filter

    cursor = (
        collection.find(query)
        .sort("dateCreated", 1)
        .batch_size(batch_size)
    )

    current_date = None
    buffer: List[Dict] = []
    for document in cursor:
        stats["read"] += 1
        normalized = _normalize_event(document)
        event_date = _event_date(document) or _event_date(normalized)
        event_id = normalized.get("id")
        if not event_id or event_date is None:
            stats["skipped"] += 1
            continue

        normalized.pop("_id", None)
        normalized["event"] = event_name
        normalized["partition_date"] = event_date.isoformat()

        if current_date is None:
            current_date = event_date
        if event_date != current_date:
            _write_partition(dataset_root, event_name, current_date, buffer, manifest_cache)
            stats["written"] += len(buffer)
            buffer = []
            current_date = event_date

        buffer.append(normalized)

    if buffer and current_date is not None:
        _write_partition(dataset_root, event_name, current_date, buffer, manifest_cache)
        stats["written"] += len(buffer)

    manifest_cache.flush()
    return stats


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    dataset_root = Path(args.dataset_root)
    dataset_root.mkdir(parents=True, exist_ok=True)

    client = MongoClient(args.mongo_uri)
    collections = args.collections or list(ISSUES.keys())

    manifest_cache = ManifestCache(dataset_root)
    date_filter = _date_filter(args)

    for event_name in collections:
        logging.info("Exporting collection '%s'", event_name)
        stats = export_collection(
            client,
            args.db,
            event_name,
            dataset_root,
            args.batch_size,
            date_filter,
            manifest_cache,
        )
        logging.info(
            "Finished %s: %d read, %d written, %d skipped",
            event_name,
            stats["read"],
            stats["written"],
            stats["skipped"],
        )


if __name__ == "__main__":
    main()
