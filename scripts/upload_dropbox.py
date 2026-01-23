#!/usr/bin/env python
"""Upload parquet files to Dropbox using the SDK."""

from __future__ import annotations

import os
import sys
import time
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
    refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN")
    if not app_key or not app_secret or not refresh_token:
        missing = [
            name
            for name, value in (
                ("DROPBOX_APP_KEY", app_key),
                ("DROPBOX_APP_SECRET", app_secret),
                ("DROPBOX_REFRESH_TOKEN", refresh_token),
            )
            if not value
        ]
        print(
            f"Dropbox credentials missing ({' '.join(missing)}); skipping Dropbox upload.",
            file=sys.stderr,
        )
        return 0

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


if __name__ == "__main__":
    raise SystemExit(main())
