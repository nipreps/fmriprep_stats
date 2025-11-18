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
import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Union

import pandas as pd
import requests
from requests.utils import parse_header_links
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

MANIFEST_FILENAME = "_manifest.parquet"


def _sanitize_key(key: str) -> str:
    return key.replace(" ", "_").replace(".", "_")


def _flatten(prefix: str, value: Any, dest: Dict[str, Any]) -> None:
    if isinstance(value, dict):
        for sub_key, sub_value in value.items():
            new_key = _sanitize_key(f"{prefix}_{sub_key}" if prefix else sub_key)
            _flatten(new_key, sub_value, dest)
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            new_key = _sanitize_key(f"{prefix}_{index}")
            _flatten(new_key, item, dest)
        return
    dest[_sanitize_key(prefix)] = value


def _normalize_event(event: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key, value in event.items():
        if key == "tags":
            continue
        if key == "_id":
            normalized["_id"] = str(value)
            continue
        _flatten(key, value, normalized)

    tags = event.get("tags", []) or []
    for tag in tags:
        tag_key = _sanitize_key(tag.get("key", ""))
        if tag_key:
            normalized[tag_key] = tag.get("value")

    normalized.pop("environment", None)
    return normalized


def _coerce_datetime(value: Any) -> Optional[datetime.datetime]:
    if isinstance(value, datetime.datetime):
        if not value.tzinfo:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value
    if not value:
        return None
    if isinstance(value, str):
        txt = value.strip()
        if txt.endswith("Z"):
            txt = txt[:-1] + "+00:00"
        try:
            return datetime.datetime.fromisoformat(txt)
        except ValueError:
            return None
    return None


def _event_date(event: Dict[str, Any]) -> Optional[datetime.date]:
    for key in ("dateCreated", "timestamp", "received", "datetime"):
        dt = _coerce_datetime(event.get(key))
        if dt:
            return dt.date()
    return None


def _partition_path(dataset_root: Union[Path, str], event_name: str, day: datetime.date) -> Path:
    root = Path(dataset_root)
    return root / event_name / f"{day.isoformat()}.parquet"


def _manifest_path(dataset_root: Union[Path, str]) -> Path:
    return Path(dataset_root) / MANIFEST_FILENAME


def _read_manifest(dataset_root: Union[Path, str]) -> pd.DataFrame:
    manifest_file = _manifest_path(dataset_root)
    if not manifest_file.exists():
        return pd.DataFrame(columns=["event", "date", "path", "rows", "min_id", "max_id"])
    return pd.read_parquet(manifest_file)


def _existing_manifest_paths(dataset_root: Union[Path, str]) -> Set[str]:
    manifest = _read_manifest(dataset_root)
    if manifest.empty:
        return set()
    return set(manifest["path"].astype(str))


def _write_manifest(dataset_root: Union[Path, str], entries: Iterable[Dict[str, Any]]) -> None:
    entries = list(entries)
    if not entries:
        return

    manifest_file = _manifest_path(dataset_root)
    manifest_file.parent.mkdir(parents=True, exist_ok=True)
    existing = _read_manifest(dataset_root)
    df_new = pd.DataFrame(entries)
    merged = pd.concat([existing, df_new], ignore_index=True)
    tmp_path = manifest_file.with_suffix(".tmp")
    merged.to_parquet(tmp_path, index=False)
    tmp_path.replace(manifest_file)


class ManifestCache:
    def __init__(self, dataset_root: Union[Path, str], flush_threshold: int = 10) -> None:
        self.dataset_root = Path(dataset_root)
        self.flush_threshold = max(1, flush_threshold)
        self._cache: List[Dict[str, Any]] = []
        self._seen_paths: Set[str] = _existing_manifest_paths(self.dataset_root)

    @property
    def seen_paths(self) -> Set[str]:
        return self._seen_paths

    def add(self, entry: Dict[str, Any]) -> None:
        path_key = entry.get("path")
        if path_key in self._seen_paths:
            return
        self._cache.append(entry)
        if len(self._cache) >= self.flush_threshold:
            self.flush()

    def flush(self) -> None:
        if not self._cache:
            return
        _write_manifest(self.dataset_root, self._cache)
        self._seen_paths.update(entry["path"] for entry in self._cache)
        self._cache.clear()


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
