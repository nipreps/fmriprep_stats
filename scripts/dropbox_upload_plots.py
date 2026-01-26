#!/usr/bin/env python3
"""Upload plot artifacts to Dropbox using refresh-token auth."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

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


def upload_file(access_token: str, dropbox_path: str, file_path: Path) -> None:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Dropbox-API-Arg": json.dumps({"path": dropbox_path, "mode": "overwrite", "mute": True}),
        "Content-Type": "application/octet-stream",
    }
    response = requests.post(
        "https://content.dropboxapi.com/2/files/upload",
        headers=headers,
        data=file_path.read_bytes(),
        timeout=300,
    )
    response.raise_for_status()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload plots to Dropbox")
    parser.add_argument("--plots-dir", required=True, help="Local plots directory")
    parser.add_argument("--dropbox-path", required=True, help="Dropbox destination path")
    parser.add_argument("--app-key", required=True, help="Dropbox app key")
    parser.add_argument("--app-secret", required=True, help="Dropbox app secret")
    parser.add_argument("--refresh-token", required=True, help="Dropbox refresh token")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    plots_dir = Path(args.plots_dir)
    dropbox_base = args.dropbox_path.rstrip("/")

    access_token = get_access_token(args.app_key, args.app_secret, args.refresh_token)

    for file_path in plots_dir.rglob("*"):
        if file_path.is_dir():
            continue
        rel_path = file_path.relative_to(plots_dir).as_posix()
        dropbox_path = f"{dropbox_base}/{rel_path}"
        upload_file(access_token, dropbox_path, file_path)


if __name__ == "__main__":
    main()
