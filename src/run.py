# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
# vi: set ft=python sts=4 ts=4 sw=4 et:
#
# Copyright 2022 The NiPreps Developers <nipreps@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# We support and encourage derived works from this project, please read
# about our expectations at
#
#     https://www.nipreps.org/community/licensing/
#
"""CLI."""

import os
import re
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

import click
from api import parallel_fetch, ISSUES, DEFAULT_MAX_ERRORS
from db import load_event, massage_versions, mongo_id_lookup, store_events
from viz import plot_performance, plot_version_stream

DEFAULT_DAYS_WINDOW = 90
DEFAULT_CHUNK_DAYS = 1


@click.group()
@click.version_option(message="fMRIPrep stats")
def cli():
    """Download stats from Sentry.io."""


def _sanitize_event_name(event_name):
    normalized = event_name.strip().lower()
    normalized = normalized.replace(os.sep, "_")
    if os.altsep:
        normalized = normalized.replace(os.altsep, "_")
    normalized = re.sub(r"[^a-z0-9_.-]+", "_", normalized)
    normalized = normalized.strip("_")
    return normalized or "event"


@cli.command()
@click.option(
    "-m",
    "--event",
    type=click.Choice(ISSUES.keys(), case_sensitive=False),
    multiple=True,
    default=("started", "success", "failed"),
    help="Which Sentry issues to fetch",
)
@click.option(
    "-S",
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]),
    default=None,
    help=f"Start date (inclusive) in YYYY-MM-DD; defaults to {DEFAULT_DAYS_WINDOW} days ago",
)
@click.option(
    "-E",
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]),
    default=None,
    help="End date (exclusive) in YYYY-MM-DD; defaults to today",
)
@click.option(
    "-D",
    "--days",
    type=int,
    default=None,
    help="End date (exclusive) in YYYY-MM-DD; defaults to today",
)
@click.option(
    "-c",
    "--chunk-days",
    type=click.IntRange(min=1),
    default=DEFAULT_CHUNK_DAYS,
    help="Number of days per parallel chunk",
)
@click.option(
    "-J",
    "--jobs",
    type=click.IntRange(min=1),
    default=None,
    help="Max number of parallel worker threads",
)
@click.option(
    "-M", "--max-errors", type=click.IntRange(min=1), default=DEFAULT_MAX_ERRORS
)
@click.option("-L", "--cached-limit", type=click.IntRange(min=1), default=None)
@click.option(
    "--store",
    type=click.Choice(["mongo", "parquet"], case_sensitive=False),
    default="mongo",
    show_default=True,
    help="Store fetched records in MongoDB or as parquet files.",
)
@click.option(
    "--print-dataframe/--no-print-dataframe",
    default=False,
    help="Print a preview of the fetched dataframe to stdout.",
)
@click.option(
    "-o",
    "--output-file",
    type=click.Path(dir_okay=False, writable=True),
    default=None,
    help=(
        "Output file path for parquet output when --store parquet is enabled. "
        "If omitted, writes to YYYY-MM-DD-<event>.parquet in the current directory."
    ),
)
def get(
    event,
    start_date,
    end_date,
    days,
    chunk_days,
    jobs,
    max_errors,
    cached_limit,
    store,
    print_dataframe,
    output_file,
):
    """Fetch events in parallel using time-window chunking."""

    token = os.getenv("SENTRY_TOKEN")
    if not token:
        click.echo("ERROR: SENTRY_TOKEN environment variable not set", err=True)
        sys.exit(1)

    now = datetime.now(timezone.utc)

    if start_date and days:
        click.echo("Warning: overriding --start-date because --days was present")
        start_date = None

    if start_date:
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=now.tzinfo)
        else:
            start_date = start_date.astimezone(now.tzinfo)
    if end_date:
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=now.tzinfo)
        else:
            end_date = end_date.astimezone(now.tzinfo)
    else:
        end_date = now

    start_date = start_date or (end_date - timedelta(days=days or DEFAULT_DAYS_WINDOW))
    start_date += timedelta(microseconds=1)

    click.echo(
        f"{now:%Y-%m-%d %H:%M:%S} [Started] "
        f"from: {start_date:%Y-%m-%d %H:%M:%S}, to: {end_date:%Y-%m-%d %H:%M:%S}"
    )

    store = store.lower()
    if store == "parquet" and output_file and len(event) > 1:
        click.echo(
            "ERROR: --output-file can only be used with a single --event to avoid overwrites.",
            err=True,
        )
        sys.exit(2)
    # Get events
    for ev in event:
        id_lookup = mongo_id_lookup(ev) if store == "mongo" else None
        _, _, records = parallel_fetch(
            event_name=ev,
            token=token,
            since=start_date,
            until=end_date,
            days_per_chunk=chunk_days,
            max_workers=jobs,
            cached_limit=cached_limit,
            max_errors=max_errors,
            id_lookup=id_lookup,
        )
        if print_dataframe:
            if records.empty:
                click.echo(f"[{ev}] No records fetched.")
            else:
                click.echo(f"[{ev}] Dataframe preview:")
                click.echo(records.head().to_string())
            continue
        if store == "mongo":
            inserted = store_events(ev, records)
            click.echo(f"[{ev}] Inserted {inserted} new records.")
        elif store == "parquet":
            if records.empty:
                click.echo(f"[{ev}] No records fetched.")
            else:
                safe_event = _sanitize_event_name(ev)
                last_complete_day = (end_date - timedelta(days=1)).date().isoformat()
                filename = output_file or f"{last_complete_day}-{safe_event}.parquet"
                destination = Path(filename)
                destination.parent.mkdir(parents=True, exist_ok=True)
                records.to_parquet(destination)
                click.echo(f"[{ev}] Wrote dataframe to {destination}.")
    click.echo(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} [Finished]")


@cli.command()
@click.option("-o", "--output-dir", type=click.Path(file_okay=False, dir_okay=True, writable=True), default=".")
@click.option("--drop-cutoff", default=None, help="Ignore versions older than this")
def plot(output_dir, drop_cutoff):
    """Generate plots using records stored in MongoDB."""
    today = datetime.now().date().strftime("%Y%m%d")
    out_perf = os.path.join(output_dir, f"{today}_weekly.png")
    out_ver = os.path.join(output_dir, f"{today}_versionstream.png")

    unique_started = load_event("started")
    unique_success = load_event("success")

    plot_performance(unique_started, unique_success, drop_cutoff=drop_cutoff, out_file=out_perf)
    click.echo(f"Saved {out_perf}.")

    started_v, success_v = massage_versions(unique_started, unique_success)
    plot_version_stream(started_v, success_v, drop_cutoff=drop_cutoff, out_file=out_ver)
    click.echo(f"Saved {out_ver}")


if __name__ == "__main__":
    """ Install entry-point """
    cli()
