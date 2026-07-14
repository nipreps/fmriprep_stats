#!/usr/bin/env python3
"""Upload plot artifacts to Dropbox using refresh-token auth."""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path

import requests

LOG = logging.getLogger("dropbox_upload_plots")

# Dropbox 429 (rate limit) and transient edge 5xx are worth retrying; 4xx are not.
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
MAX_BACKOFF_SECONDS = 60.0


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    *,
    max_attempts: int,
    base_delay: float,
    **kwargs,
) -> requests.Response:
    """Issue a request, retrying transient failures with exponential backoff.

    Retries on HTTP 429/5xx and on connection/timeout errors. On 429 the
    server's ``Retry-After`` header is honored when present; otherwise the wait
    is ``base_delay * 2 ** (attempt - 1)``, capped at ``MAX_BACKOFF_SECONDS``.
    Returns the final response (possibly non-OK if retries were exhausted).
    """
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.request(method, url, **kwargs)
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            if attempt >= max_attempts:
                raise
            wait = min(base_delay * 2 ** (attempt - 1), MAX_BACKOFF_SECONDS)
            LOG.warning(
                "%s %s failed (%s); retry %d/%d in %.1fs",
                method,
                url,
                exc.__class__.__name__,
                attempt,
                max_attempts,
                wait,
            )
            time.sleep(wait)
            continue

        if response.status_code in RETRYABLE_STATUS and attempt < max_attempts:
            retry_after = response.headers.get("Retry-After")
            wait = base_delay * 2 ** (attempt - 1)
            if retry_after is not None:
                try:
                    wait = float(retry_after)
                except ValueError:
                    pass
            wait = min(wait, MAX_BACKOFF_SECONDS)
            LOG.warning(
                "%s %s -> %d; retry %d/%d in %.1fs",
                method,
                url,
                response.status_code,
                attempt,
                max_attempts,
                wait,
            )
            time.sleep(wait + 0.1)
            continue

        return response


def get_access_token(
    session: requests.Session,
    app_key: str,
    app_secret: str,
    refresh_token: str,
    *,
    max_attempts: int,
    base_delay: float,
) -> str:
    response = _request_with_retry(
        session,
        "POST",
        "https://api.dropboxapi.com/oauth2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": app_key,
            "client_secret": app_secret,
        },
        timeout=30,
        max_attempts=max_attempts,
        base_delay=base_delay,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["access_token"]


def upload_file(
    session: requests.Session,
    access_token: str,
    dropbox_path: str,
    file_path: Path,
    *,
    max_attempts: int,
    base_delay: float,
) -> None:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Dropbox-API-Arg": json.dumps(
            {"path": dropbox_path, "mode": "overwrite", "mute": True}
        ),
        "Content-Type": "application/octet-stream",
    }
    response = _request_with_retry(
        session,
        "POST",
        "https://content.dropboxapi.com/2/files/upload",
        headers=headers,
        data=file_path.read_bytes(),
        timeout=300,
        max_attempts=max_attempts,
        base_delay=base_delay,
    )
    if not response.ok:
        raise requests.exceptions.HTTPError(
            f"{response.status_code} for {dropbox_path}: {response.text or '<empty>'}",
            response=response,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload plots to Dropbox")
    parser.add_argument("--plots-dir", required=True, help="Local plots directory")
    parser.add_argument(
        "--dropbox-path", required=True, help="Dropbox destination path"
    )
    parser.add_argument("--app-key", required=True, help="Dropbox app key")
    parser.add_argument("--app-secret", required=True, help="Dropbox app secret")
    parser.add_argument("--refresh-token", required=True, help="Dropbox refresh token")
    parser.add_argument(
        "--retries",
        type=int,
        default=_env_int("DROPBOX_UPLOAD_RETRIES", 5),
        help="Max attempts per request (env: DROPBOX_UPLOAD_RETRIES)",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=_env_float("DROPBOX_UPLOAD_RETRY_DELAY", 2.0),
        help="Base backoff seconds (env: DROPBOX_UPLOAD_RETRY_DELAY)",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO
    )
    args = parse_args()
    plots_dir = Path(args.plots_dir)
    dropbox_base = args.dropbox_path.rstrip("/")

    session = requests.Session()
    retry_kwargs = {
        "max_attempts": max(1, args.retries),
        "base_delay": args.retry_delay,
    }

    access_token = get_access_token(
        session, args.app_key, args.app_secret, args.refresh_token, **retry_kwargs
    )

    failures: list[tuple[str, str]] = []
    for file_path in plots_dir.rglob("*"):
        if file_path.is_dir():
            continue
        rel_path = file_path.relative_to(plots_dir).as_posix()
        dropbox_path = f"{dropbox_base}/{rel_path}"
        try:
            upload_file(session, access_token, dropbox_path, file_path, **retry_kwargs)
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            failures.append((dropbox_path, str(exc)))
            LOG.error("upload FAILED for %s: %s", dropbox_path, exc)
            continue
        LOG.info("uploaded %s -> %s", file_path, dropbox_path)

    # Plots are the product of the run — if any could not be uploaded after
    # retries, fail so the failure is visible rather than silently dropped.
    if failures:
        LOG.error("upload failures (%d):", len(failures))
        for path, msg in failures:
            LOG.error("  %s -> %s", path, msg)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
