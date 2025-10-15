#!/usr/bin/env python3
"""Convert MongoDB event collections into a partitioned Parquet dataset."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import click
import pandas as pd
from pymongo import MongoClient
from uuid import uuid4

from src import api

DEFAULT_BATCH_SIZE = 1000


def _normalize_records(records: Iterable[Dict]) -> pd.DataFrame:
    """Return a flattened dataframe for *records*."""

    return pd.json_normalize(list(records), sep=".")


@click.command()
@click.option(
    "--mongo-uri",
    default="mongodb://localhost:27017",
    show_default=True,
    help="MongoDB connection URI.",
)
@click.option(
    "--db-name",
    default="fmriprep_stats",
    show_default=True,
    help="MongoDB database name.",
)
@click.option(
    "--batch-size",
    type=click.IntRange(min=1),
    default=DEFAULT_BATCH_SIZE,
    show_default=True,
    help="Number of events to buffer before writing Parquet partitions.",
)
@click.argument("dataset_root", type=click.Path(path_type=Path))
def main(mongo_uri: str, db_name: str, batch_size: int, dataset_root: Path) -> None:
    """Stream MongoDB events into a partitioned Parquet dataset."""

    dataset_root = dataset_root.resolve()
    dataset_root.mkdir(parents=True, exist_ok=True)

    manifest_path = api._manifest_path(dataset_root)
    manifest = api._load_manifest(manifest_path)
    manifest_cache = api._load_manifest_cache(manifest)

    client = MongoClient(mongo_uri)
    db = client[db_name]

    buffers: Dict[Tuple[str, Path], List[Dict]] = defaultdict(list)
    pending_records = 0
    totals = {event: 0 for event in api.ISSUES}

    def flush_buffers() -> None:
        nonlocal pending_records, manifest

        if pending_records == 0:
            return

        new_manifest_rows: List[Dict] = []

        for (event_name, partition_dir), entries in list(buffers.items()):
            if not entries:
                continue

            partition_dir.mkdir(parents=True, exist_ok=True)
            records = [entry["record"] for entry in entries]
            df = _normalize_records(records)
            if df.empty:
                continue

            part_path = partition_dir / f"part-{uuid4().hex}.parquet"
            df.to_parquet(part_path, index=False)
            relative = str(part_path.relative_to(dataset_root))

            totals[event_name] += len(entries)
            for entry in entries:
                new_manifest_rows.append(
                    {
                        "event": event_name,
                        "id": entry["record"]["id"],
                        "date": entry["date"].isoformat(),
                        "path": relative,
                    }
                )

        if new_manifest_rows:
            manifest = pd.concat(
                [manifest, pd.DataFrame(new_manifest_rows)], ignore_index=True
            )
            api._write_manifest(manifest_path, manifest)
            api._update_manifest_cache(manifest_cache, new_manifest_rows)

        buffers.clear()
        pending_records = 0

    try:
        for event_name in api.ISSUES:
            click.echo(f"Migrating '{event_name}' events…")

            collection = db[event_name]
            cursor = collection.find({}, batch_size=batch_size)

            for document in cursor:
                record = api._normalize_event(document)
                event_id = record.get("id")
                if not event_id:
                    continue

                cache = manifest_cache.setdefault(event_name, set())
                if event_id in cache:
                    continue

                event_date = api._event_date(record)
                if event_date is None:
                    continue

                partition_dir = api._partition_path(dataset_root, event_name, event_date)
                buffers[(event_name, partition_dir)].append(
                    {"record": record, "date": event_date}
                )
                cache.add(event_id)
                pending_records += 1

                if pending_records >= batch_size:
                    flush_buffers()

    finally:
        flush_buffers()
        client.close()

    click.echo("Migration summary:")
    for event_name, count in totals.items():
        click.echo(f"  {event_name}: {count} new event(s) written")


if __name__ == "__main__":
    main()
