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
import sys
from datetime import datetime, timezone, timedelta

import click
from api import parallel_fetch, ISSUES, DEFAULT_MAX_ERRORS

DEFAULT_DAYS_WINDOW = 90
DEFAULT_CHUNK_DAYS = 1


@click.group()
@click.version_option(message="fMRIPrep stats")
def cli():
    """Download stats from Sentry.io."""


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
    "-s",
    "--since",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help=f"Start date (inclusive) in YYYY-MM-DD; defaults to {DEFAULT_DAYS_WINDOW} days ago",
)
@click.option(
    "-u",
    "--until",
    type=click.DateTime(formats=["%Y-%m-%d"]),
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
def get(event, since, until, chunk_days, jobs, max_errors, cached_limit):
    """Fetch events in parallel using time-window chunking."""

    token = os.getenv("SENTRY_TOKEN")
    if not token:
        click.echo("ERROR: SENTRY_TOKEN environment variable not set", err=True)
        sys.exit(1)

    now = datetime.now(timezone.utc)

    since = since or (now - timedelta(days=DEFAULT_DAYS_WINDOW))
    until = until or now

    click.echo(f"{now:%Y-%m-%d %H:%M:%S} [Started]")

    # Get events
    for ev in event:
        parallel_fetch(
            event_name=ev,
            token=token,
            since=since,
            until=until,
            days_per_chunk=chunk_days,
            max_workers=jobs,
            cached_limit=cached_limit,
            max_errors=max_errors,
        )
    click.echo(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} [Finished]")


if __name__ == "__main__":
    """ Install entry-point """
    cli()
