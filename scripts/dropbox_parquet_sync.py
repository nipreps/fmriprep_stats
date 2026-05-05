#!/usr/bin/env python3
"""Sync parquet files from Dropbox using refresh-token auth."""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Iterable

import requests

LOG = logging.getLogger("dropbox_parquet_sync")


def get_access_token(app_key: str, app_secret: str, refresh_token: str) -> str:
    response = requests.post(
        "https://api.dropboxapi.com/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": app_key,
            "client_secret": app_secret,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["access_token"]


def list_folder_entries(access_token: str, path: str) -> Iterable[dict]:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    entries = []
    payload = {"path": path}
    url = "https://api.dropboxapi.com/2/files/list_folder"

    while True:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        data = response.json()
        entries.extend(data.get("entries", []))
        if data.get("has_more"):
            payload = {"cursor": data["cursor"]}
            url = "https://api.dropboxapi.com/2/files/list_folder/continue"
        else:
            break

    return entries


def download_file(access_token: str, dropbox_path: str, destination: Path) -> None:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Dropbox-API-Arg": json.dumps({"path": dropbox_path}),
    }
    response = requests.post(
        "https://content.dropboxapi.com/2/files/download",
        headers=headers,
        timeout=300,
    )
    if not response.ok:
        # Surface Dropbox's specific error tag (e.g. path/not_found) which lives
        # in the JSON body — raise_for_status() alone hides it.
        raise requests.exceptions.HTTPError(
            f"{response.status_code} for {dropbox_path}: {response.text or '<empty>'}",
            response=response,
        )
    destination.write_bytes(response.content)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync parquet files from Dropbox")
    parser.add_argument("--parquet-dir", required=True, help="Local parquet directory")
    parser.add_argument("--dropbox-path", required=True, help="Dropbox folder path")
    parser.add_argument("--app-key", required=True, help="Dropbox app key")
    parser.add_argument("--app-secret", required=True, help="Dropbox app secret")
    parser.add_argument("--refresh-token", required=True, help="Dropbox refresh token")
    return parser.parse_args()


def load_metadata(metadata_path: Path) -> dict[str, dict[str, str | int]]:
    if not metadata_path.exists():
        return {}
    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_metadata(metadata_path: Path, metadata: dict[str, dict[str, str | int]]) -> None:
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")


def main() -> int:
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO
    )
    args = parse_args()
    parquet_dir = Path(args.parquet_dir)
    parquet_dir.mkdir(parents=True, exist_ok=True)
    dropbox_path = args.dropbox_path.rstrip("/")
    metadata_path = parquet_dir / ".dropbox_metadata.json"
    cached_metadata = load_metadata(metadata_path)

    access_token = get_access_token(args.app_key, args.app_secret, args.refresh_token)
    entries = list(list_folder_entries(access_token, dropbox_path))
    LOG.info(
        "Listed %d entries at Dropbox path %r", len(entries), dropbox_path or "/"
    )

    fetched = skipped = failed = 0
    failures: list[tuple[str, str]] = []

    for entry in entries:
        if entry.get(".tag") != "file":
            continue
        dropbox_file = entry["path_lower"]
        rel_path = entry["path_display"][len(dropbox_path) :].lstrip("/")
        local_path = parquet_dir / rel_path
        local_path.parent.mkdir(parents=True, exist_ok=True)
        expected_rev = entry.get("rev")
        expected_size = entry.get("size")
        cached_entry = cached_metadata.get(dropbox_file, {})
        if local_path.exists() and expected_rev and cached_entry.get("rev") == expected_rev:
            skipped += 1
            continue
        try:
            download_file(access_token, dropbox_file, local_path)
        except requests.exceptions.HTTPError as exc:
            failed += 1
            failures.append((dropbox_file, str(exc)))
            LOG.error("download FAILED for %s: %s", dropbox_file, exc)
            continue
        cached_metadata[dropbox_file] = {
            "rev": expected_rev or "",
            "size": expected_size or 0,
        }
        fetched += 1
        if fetched % 100 == 0:
            LOG.info(
                "progress: fetched=%d skipped=%d failed=%d",
                fetched,
                skipped,
                failed,
            )

    write_metadata(metadata_path, cached_metadata)

    LOG.info(
        "summary: fetched=%d skipped(cached)=%d failed=%d",
        fetched,
        skipped,
        failed,
    )
    if failures:
        LOG.error("download failures (%d):", len(failures))
        for path, msg in failures[:25]:
            LOG.error("  %s -> %s", path, msg)
        if len(failures) > 25:
            LOG.error("  ... and %d more", len(failures) - 25)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
