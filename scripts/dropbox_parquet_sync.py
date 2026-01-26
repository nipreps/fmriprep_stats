#!/usr/bin/env python3
"""Sync parquet files from Dropbox using refresh-token auth."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import requests


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
    response.raise_for_status()
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


def main() -> None:
    args = parse_args()
    parquet_dir = Path(args.parquet_dir)
    parquet_dir.mkdir(parents=True, exist_ok=True)
    dropbox_path = args.dropbox_path.rstrip("/")
    metadata_path = parquet_dir / ".dropbox_metadata.json"
    cached_metadata = load_metadata(metadata_path)

    access_token = get_access_token(args.app_key, args.app_secret, args.refresh_token)
    entries = list_folder_entries(access_token, dropbox_path)

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
            continue
        download_file(access_token, dropbox_file, local_path)
        cached_metadata[dropbox_file] = {
            "rev": expected_rev or "",
            "size": expected_size or 0,
        }

    write_metadata(metadata_path, cached_metadata)


if __name__ == "__main__":
    main()
