#!/usr/bin/env python
"""Upload parquet files to Dropbox using the SDK."""

from __future__ import annotations

import os
import sys
import time
import json
import urllib.parse
import urllib.request
from pathlib import Path

import dropbox
from dropbox import exceptions, files


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return int(value)


def main() -> int:
    app_key = os.getenv("DROPBOX_APP_KEY")
    app_secret = os.getenv("DROPBOX_APP_SECRET")
    access_code = os.getenv("DROPBOX_APP_ACCESS_CODE")
    if not app_key or not app_secret or not access_code:
        missing = [
            name
            for name, value in (
                ("DROPBOX_APP_KEY", app_key),
                ("DROPBOX_APP_SECRET", app_secret),
                ("DROPBOX_APP_ACCESS_CODE", access_code),
            )
            if not value
        ]
        print(
            f"Dropbox credentials missing ({' '.join(missing)}); skipping Dropbox upload.",
            file=sys.stderr,
        )
        return 0

    refresh_token = _exchange_refresh_token(app_key, app_secret, access_code)

    retries = _env_int("DROPBOX_UPLOAD_RETRIES", 3)
    retry_delay = _env_int("DROPBOX_UPLOAD_RETRY_DELAY", 2)

    output_dir = Path("output")
    files_to_upload = sorted(output_dir.glob("*.parquet"))
    if not files_to_upload:
        print("No parquet files found to upload.", file=sys.stderr)
        return 1

    client = dropbox.Dropbox(
        oauth2_refresh_token=refresh_token,
        app_key=app_key,
        app_secret=app_secret,
    )

    failed = False
    for file_path in files_to_upload:
        dropbox_path = f"/{file_path.name}"
        success = False
        for attempt in range(1, retries + 1):
            try:
                with file_path.open("rb") as handle:
                    client.files_upload(
                        handle.read(),
                        dropbox_path,
                        mode=files.WriteMode.overwrite,
                    )
                print(f"[Dropbox] Uploaded {file_path} -> {dropbox_path}.")
                success = True
                break
            except exceptions.ApiError as exc:
                print(
                    f"[Dropbox] Upload attempt {attempt}/{retries} failed for {file_path}: {exc}",
                    flush=True,
                )
            except Exception as exc:
                print(
                    f"[Dropbox] Upload attempt {attempt}/{retries} failed for {file_path}: {exc}",
                    flush=True,
                )
            if attempt < retries:
                time.sleep(retry_delay * attempt)
        if not success:
            print(f"[Dropbox] Failed to upload {file_path} after {retries} attempts.")
            failed = True

    return 1 if failed else 0


def _exchange_refresh_token(app_key: str, app_secret: str, access_code: str) -> str:
    payload = urllib.parse.urlencode(
        {
            "code": access_code,
            "grant_type": "authorization_code",
            "client_id": app_key,
            "client_secret": app_secret,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        "https://api.dropbox.com/oauth2/token",
        data=payload,
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        body = response.read().decode("utf-8")
    try:
        data = json.loads(body)
    except ValueError as exc:
        raise SystemExit(f"Failed to parse Dropbox token response: {exc}") from exc
    token = data.get("refresh_token")
    if not token:
        raise SystemExit(f"No refresh_token in response: {body}")
    return token


if __name__ == "__main__":
    raise SystemExit(main())
