#!/usr/bin/env python3
"""Export daily Sentry events to a single parquet file."""

import argparse
import datetime as dt
import os
from time import sleep

import pandas as pd
import requests
from requests.utils import parse_header_links

ISSUES = {
    "success": "758615130",
    "started": "540334560",
    "failed": "848853674",
}

DEFAULT_MAX_ERRORS = 5


def _to_sentry_time(timepoint: dt.datetime) -> str:
    """Format datetime in Sentry's expected ISO-8601 UTC format."""

    return timepoint.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _flatten_event_tags(event: dict) -> dict:
    flattened = {
        f"{tag['key'].replace('.', '_')}": tag["value"] for tag in event.pop("tags")
    }
    event.pop("environment", None)
    event.update(flattened)
    return event


def fetch_events(
    event_name: str,
    token: str,
    window_start: dt.datetime,
    window_end: dt.datetime,
    max_errors: int = DEFAULT_MAX_ERRORS,
) -> list[dict]:
    """Fetch all events for a given issue in the provided time window."""

    issue_id = ISSUES[event_name]
    start_iso = _to_sentry_time(window_start)
    end_iso = _to_sentry_time(window_end)
    base_url = (
        f"https://sentry.io/api/0/issues/{issue_id}/events/"
        f"?start={start_iso}&end={end_iso}"
    )

    headers = {"Authorization": f"Bearer {token}"}
    cursor = None
    errors = 0
    events: list[dict] = []

    while True:
        url = base_url if cursor is None else f"{base_url}&cursor={cursor}"
        req = requests.get(url, headers=headers, timeout=30)

        if req.status_code == 429:
            wait = int(req.headers.get("Retry-After", 2**errors))
            sleep(wait + 0.1)
            errors += 1
            if errors > max_errors:
                break
            continue

        if not req.ok:
            errors += 1
            if errors > max_errors:
                break
            sleep(errors)
            continue

        errors = 0
        page_events = [
            event
            for event in req.json()
            if {"key": "environment", "value": "prod"} in event["tags"]
        ]
        for event in page_events:
            flattened = _flatten_event_tags(event)
            flattened["status"] = event_name
            events.append(flattened)

        link_header = req.headers.get("Link", "")
        next_link = None
        if link_header:
            for link in parse_header_links(link_header):
                if link.get("rel") == "next":
                    next_link = link
                    break

        if not next_link or next_link.get("results") == "false":
            break

        cursor = next_link.get("cursor")

    return events


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch daily Sentry events and write a parquet file."
    )
    parser.add_argument(
        "--date",
        required=True,
        help="UTC date to fetch in YYYYMMDD format.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to write the parquet file.",
    )
    parser.add_argument(
        "--token",
        default=os.getenv("SENTRY_TOKEN"),
        help="Sentry API token (defaults to SENTRY_TOKEN env var).",
    )
    parser.add_argument(
        "--max-errors",
        type=int,
        default=DEFAULT_MAX_ERRORS,
        help="Maximum consecutive request errors before aborting.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if not args.token:
        raise SystemExit("SENTRY_TOKEN is required (via --token or env var).")

    try:
        target_date = dt.datetime.strptime(args.date, "%Y%m%d")
    except ValueError as exc:
        raise SystemExit("--date must be in YYYYMMDD format.") from exc

    window_start = target_date.replace(tzinfo=dt.timezone.utc)
    window_end = window_start + dt.timedelta(days=1)

    all_events: list[dict] = []
    for event_name in ISSUES:
        all_events.extend(
            fetch_events(
                event_name,
                args.token,
                window_start,
                window_end,
                max_errors=args.max_errors,
            )
        )

    if all_events:
        columns = sorted({key for record in all_events for key in record.keys()})
        df = pd.DataFrame(all_events, columns=columns)
    else:
        df = pd.DataFrame(columns=["status"])

    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    df.to_parquet(args.output, index=False)


if __name__ == "__main__":
    main()
