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
"""Fetching fMRIPrep statistics from Sentry."""

import os
import sys
from time import sleep
import requests
from requests.utils import parse_header_links
import datetime
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

DEFAULT_MAX_ERRORS = 5
ISSUES = {
    "success": "758615130",
    "started": "540334560",
    "failed": "848853674",
    "no_disk": "767302904",
    "sigkill": "854282951",
}


def filter_new(events, collection):
    """Return the subset of *events* not already cached in *collection*."""

    ids = [e["id"] for e in events]
    if not ids:
        return []

    cached_ids = set(collection.distinct("id", {"id": {"$in": ids}}))

    new_docs = []
    for event in events:
        if event["id"] in cached_ids:
            continue
        event.update(
            {
                f"{tag['key'].replace('.', '_')}": tag["value"]
                for tag in event.pop("tags")
            }
        )
        event.pop("environment", None)
        new_docs.append(event)

    return new_docs


def _to_sentry_time(dt):
    # drop microseconds, force UTC 'Z' suffix
    return dt.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_window(
    event_name, token, window_start, window_end, max_errors=None, cached_limit=None
):
    """Fetch one time-window's worth of pages, sequentially paging until done."""

    max_errors = max_errors or DEFAULT_MAX_ERRORS

    issue_id = ISSUES[event_name]
    start_iso = _to_sentry_time(window_start)
    end_iso = _to_sentry_time(window_end)

    # build the base URL with your date filters
    base_url = (
        f"https://sentry.io/api/0/issues/{issue_id}/events/"
        f"?start={start_iso}&end={end_iso}"
    )

    db = MongoClient().fmriprep_stats[event_name]
    db.create_index("id", unique=True)
    cursor = None
    errors = 0
    consecutive_cached = 0
    url = base_url

    new_records = 0
    total_records = 0

    headers = {"Authorization": f"Bearer {token}"}

    while True:
        if cursor:
            url = base_url + f"&cursor={cursor}"

        req = requests.get(url, headers=headers)
        if req.status_code == 429:
            # parse Retry‑After or use exponential backoff
            wait = int(req.headers.get("Retry-After", 2**errors))
            sleep(wait + 0.1)
            errors += 1
            if errors > max_errors:
                print("")
                print(f"[{event_name}][{window_start}] too many 429s; abort")
                break
            continue
        elif not req.ok:
            errors += 1
            if errors > max_errors:
                print("")
                print(
                    f"[{event_name}][{window_start}] errors: {req.status_code}; abort"
                )
                break
            sleep(errors)  # simple backoff
            continue

        errors = 0
        events = [
            e
            for e in req.json()
            if {"key": "environment", "value": "prod"} in e["tags"]
        ]
        new_docs = filter_new(events, db)

        new_records += len(new_docs)
        total_records += len(events)

        if new_docs:
            db.insert_many(new_docs)
            consecutive_cached = 0
        else:
            consecutive_cached += 1
            if cached_limit and consecutive_cached >= cached_limit:
                break

        # look at Link header for next cursor or end
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

    return new_records, total_records


def parallel_fetch(
    event_name,
    token,
    since,
    until,
    max_workers=None,
    days_per_chunk=1,
    cached_limit=None,
    max_errors=None,
):
    """Scatter a series of single-day windows in parallel."""

    from tqdm import tqdm

    if not max_workers or max_workers < 1:
        max_workers = os.cpu_count()

    windows = []
    cur = until
    while cur > since:
        nxt = max(cur - datetime.timedelta(days=days_per_chunk), since)
        windows.append((nxt, cur))
        cur = nxt

    kwargs = {
        "cached_limit": cached_limit,
        "max_errors": max_errors,
    }

    total_chunks = len(windows)
    sum_new = 0
    sum_total = 0
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = [
            exe.submit(fetch_window, event_name, token, w0, w1, **kwargs)
            for (w0, w1) in windows
        ]
        for fut in tqdm(
            as_completed(futures),
            total=total_chunks,
            desc=f"Fetching {event_name}",
            unit=f"{days_per_chunk} day(s)",
        ):
            try:
                new_rec, tot_rec = fut.result()
                sum_new += new_rec
                sum_total += tot_rec
            except Exception:
                print("chunk raised", sys.exc_info()[0])

    # final summary
    print(
        f"[{event_name}] Finished {total_chunks} chunk(s): "
        f"{sum_new} new records inserted, {sum_total} total events fetched."
    )
