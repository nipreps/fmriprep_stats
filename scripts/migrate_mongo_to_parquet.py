#!/usr/bin/env python3
"""Convert MongoDB event collections into a partitioned Parquet dataset."""

from __future__ import annotations

import datetime
import hashlib
import shutil
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List

import click
import pandas as pd
from pymongo import MongoClient
from uuid import uuid4

from src import api

DEFAULT_BATCH_SIZE = 1000
TMP_SUBDIR = ".mongo_migrate_tmp"


@dataclass(frozen=True)
class PartitionTarget:
    """Describe the output Parquet file for a batch of events."""

    event: str
    partition_dir: Path
    label: str
    digest: str

    @property
    def filename(self) -> str:
        return f"part-{self.label}_{self.digest}.parquet"

    @property
    def path(self) -> Path:
        return self.partition_dir / self.filename


def _normalize_records(records: Iterable[Dict]) -> pd.DataFrame:
    """Return a flattened dataframe for *records*."""

    return pd.json_normalize(list(records), sep=".")


def _partition_target(
    dataset_root: Path,
    event_name: str,
    event_date: datetime.date,
    frequency: str,
) -> PartitionTarget:
    """Return the output location for *event_name* at *event_date*."""

    if frequency == "week":
        iso = event_date.isocalendar()
        week_start = event_date - datetime.timedelta(days=event_date.weekday())
        label = f"{iso.year}-W{iso.week:02d}"
        partition_dir = Path(dataset_root) / event_name / f"week={week_start:%Y-%m-%d}"
    else:
        label = f"{event_date:%Y-%m-%d}"
        partition_dir = api._partition_path(dataset_root, event_name, event_date)

    digest_input = f"{event_name}|{label}".encode("utf-8")
    digest = hashlib.sha1(digest_input).hexdigest()[:8]

    return PartitionTarget(event_name, partition_dir, label, digest)


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
@click.option(
    "--partition-frequency",
    type=click.Choice(["day", "week"], case_sensitive=False),
    default="day",
    show_default=True,
    help=(
        "Granularity of the Parquet part files. Use 'week' to aggregate larger files "
        "per ISO week."
    ),
)
@click.argument("dataset_root", type=click.Path(path_type=Path))
def main(
    mongo_uri: str,
    db_name: str,
    batch_size: int,
    partition_frequency: str,
    dataset_root: Path,
) -> None:
    """Stream MongoDB events into a partitioned Parquet dataset."""

    dataset_root = dataset_root.resolve()
    dataset_root.mkdir(parents=True, exist_ok=True)
    partition_frequency = partition_frequency.lower()

    manifest_path = api._manifest_path(dataset_root)
    manifest = api._load_manifest(manifest_path)
    manifest_cache = api._load_manifest_cache(manifest)

    client = MongoClient(mongo_uri)
    db = client[db_name]

    buffers: DefaultDict[PartitionTarget, List[Dict]] = defaultdict(list)
    manifest_rows: DefaultDict[PartitionTarget, List[Dict]] = defaultdict(list)
    partial_files: DefaultDict[PartitionTarget, List[Path]] = defaultdict(list)
    pending_records = 0
    totals = {event: 0 for event in api.ISSUES}

    tmp_root = dataset_root / TMP_SUBDIR

    def flush_buffers() -> None:
        nonlocal pending_records, manifest

        if pending_records == 0:
            return

        for target, entries in list(buffers.items()):
            if not entries:
                continue

            records = [entry["record"] for entry in entries]
            df = _normalize_records(records)
            if df.empty:
                continue

            tmp_root.mkdir(parents=True, exist_ok=True)
            tmp_dir = tmp_root / target.event
            tmp_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = tmp_dir / f"{target.label}-{uuid4().hex}-{len(partial_files[target])}.parquet"
            df.to_parquet(tmp_path, index=False)
            partial_files[target].append(tmp_path)

            relative = str(target.path.relative_to(dataset_root))
            for entry in entries:
                manifest_rows[target].append(
                    {
                        "event": target.event,
                        "id": entry["record"]["id"],
                        "date": entry["date"].isoformat(),
                        "path": relative,
                    }
                )

        buffers.clear()
        pending_records = 0

    def finalize_partitions() -> None:
        nonlocal manifest

        if not partial_files:
            return

        new_manifest_rows: List[Dict] = []

        for target, temp_paths in list(partial_files.items()):
            if not temp_paths:
                continue

            frames: List[pd.DataFrame] = []
            final_path = target.path
            if final_path.exists():
                frames.append(pd.read_parquet(final_path))

            for tmp_path in temp_paths:
                frames.append(pd.read_parquet(tmp_path))

            if not frames:
                continue

            combined = pd.concat(frames, ignore_index=True)
            final_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_output = final_path.with_suffix(final_path.suffix + ".tmp")
            combined.to_parquet(tmp_output, index=False)
            tmp_output.replace(final_path)

            for tmp_path in temp_paths:
                if tmp_path.exists():
                    tmp_path.unlink()

            new_manifest_rows.extend(manifest_rows.get(target, []))

        if new_manifest_rows:
            manifest = pd.concat(
                [manifest, pd.DataFrame(new_manifest_rows)], ignore_index=True
            )
            api._write_manifest(manifest_path, manifest)
            api._update_manifest_cache(manifest_cache, new_manifest_rows)

        partial_files.clear()
        manifest_rows.clear()

        if tmp_root.exists():
            shutil.rmtree(tmp_root)

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

                target = _partition_target(
                    dataset_root, event_name, event_date, partition_frequency
                )
                buffers[target].append(
                    {"record": record, "date": event_date}
                )
                cache.add(event_id)
                pending_records += 1
                totals[event_name] += 1

                if pending_records >= batch_size:
                    flush_buffers()

    finally:
        flush_buffers()
        finalize_partitions()
        client.close()

    click.echo("Migration summary:")
    for event_name, count in totals.items():
        click.echo(f"  {event_name}: {count} new event(s) written")


if __name__ == "__main__":
    main()
