#!/usr/bin/env python3
"""Sync parquet files from Dropbox using refresh-token auth."""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Iterable

import requests

LOG = logging.getLogger("dropbox_parquet_sync")

# Statuses worth retrying: Dropbox 429 (rate limit / traffic cap) and transient
# 5xx from the edge. 4xx (e.g. 404 path/not_found, 401 auth) are permanent.
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
# Never sleep longer than this between attempts, regardless of Retry-After.
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
    Returns the final :class:`requests.Response` (which may still be non-OK if
    retries were exhausted); re-raises the network error only when every attempt
    failed to get a response at all.
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
            wait = _retry_wait(response, base_delay, attempt)
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


def _retry_wait(response: requests.Response, base_delay: float, attempt: int) -> float:
    """Backoff seconds for a retryable response, honoring Retry-After on 429."""
    retry_after = response.headers.get("Retry-After")
    if retry_after is not None:
        try:
            return min(float(retry_after), MAX_BACKOFF_SECONDS)
        except ValueError:
            pass
    return min(base_delay * 2 ** (attempt - 1), MAX_BACKOFF_SECONDS)


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


def list_folder_entries(
    session: requests.Session,
    access_token: str,
    path: str,
    *,
    max_attempts: int,
    base_delay: float,
) -> Iterable[dict]:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    entries = []
    payload = {"path": path}
    url = "https://api.dropboxapi.com/2/files/list_folder"

    while True:
        response = _request_with_retry(
            session,
            "POST",
            url,
            headers=headers,
            json=payload,
            timeout=30,
            max_attempts=max_attempts,
            base_delay=base_delay,
        )
        response.raise_for_status()
        data = response.json()
        entries.extend(data.get("entries", []))
        if data.get("has_more"):
            payload = {"cursor": data["cursor"]}
            url = "https://api.dropboxapi.com/2/files/list_folder/continue"
        else:
            break

    return entries


def download_file(
    session: requests.Session,
    access_token: str,
    dropbox_path: str,
    destination: Path,
    *,
    max_attempts: int,
    base_delay: float,
) -> None:
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Dropbox-API-Arg": json.dumps({"path": dropbox_path}),
    }
    response = _request_with_retry(
        session,
        "POST",
        "https://content.dropboxapi.com/2/files/download",
        headers=headers,
        timeout=300,
        max_attempts=max_attempts,
        base_delay=base_delay,
    )
    if not response.ok:
        # Surface Dropbox's specific error tag (e.g. path/not_found) which lives
        # in the JSON body — raise_for_status() alone hides it. The response is
        # attached so callers can classify transient (429/5xx) vs permanent.
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
    parser.add_argument(
        "--retries",
        type=int,
        default=_env_int("DROPBOX_SYNC_RETRIES", 5),
        help="Max attempts per request (env: DROPBOX_SYNC_RETRIES)",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=_env_float("DROPBOX_SYNC_RETRY_DELAY", 2.0),
        help="Base backoff seconds (env: DROPBOX_SYNC_RETRY_DELAY)",
    )
    parser.add_argument(
        "--request-pacing",
        type=float,
        default=_env_float("DROPBOX_SYNC_REQUEST_PACING", 0.1),
        help="Seconds to sleep between downloads (env: DROPBOX_SYNC_REQUEST_PACING)",
    )
    return parser.parse_args()


def load_metadata(metadata_path: Path) -> dict[str, dict[str, str | int]]:
    if not metadata_path.exists():
        return {}
    try:
        return json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_metadata(
    metadata_path: Path, metadata: dict[str, dict[str, str | int]]
) -> None:
    metadata_path.write_text(
        json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8"
    )


def _is_transient(exc: Exception) -> bool:
    """A download error is transient if it's a network blip or a 429/5xx."""
    if isinstance(
        exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)
    ):
        return True
    response = getattr(exc, "response", None)
    return response is not None and response.status_code in RETRYABLE_STATUS


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

    session = requests.Session()
    retry_kwargs = {
        "max_attempts": max(1, args.retries),
        "base_delay": args.retry_delay,
    }

    # Auth and listing failures are catastrophic — after exhausting retries the
    # raised exception propagates and the job exits non-zero.
    access_token = get_access_token(
        session, args.app_key, args.app_secret, args.refresh_token, **retry_kwargs
    )
    entries = list(
        list_folder_entries(session, access_token, dropbox_path, **retry_kwargs)
    )
    LOG.info("Listed %d entries at Dropbox path %r", len(entries), dropbox_path or "/")

    fetched = skipped = failed = 0
    transient_failures: list[tuple[str, str]] = []
    permanent_failures: list[tuple[str, str]] = []

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
        if (
            local_path.exists()
            and expected_rev
            and cached_entry.get("rev") == expected_rev
        ):
            skipped += 1
            continue
        try:
            download_file(
                session, access_token, dropbox_file, local_path, **retry_kwargs
            )
        except (
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            failed += 1
            bucket = transient_failures if _is_transient(exc) else permanent_failures
            bucket.append((dropbox_file, str(exc)))
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
        if args.request_pacing > 0:
            time.sleep(args.request_pacing)

    # Persist whatever we did fetch so a partial pull converges on the next run.
    write_metadata(metadata_path, cached_metadata)

    LOG.info(
        "summary: fetched=%d skipped(cached)=%d failed=%d", fetched, skipped, failed
    )

    for label, bucket in (
        ("transient", transient_failures),
        ("permanent", permanent_failures),
    ):
        if not bucket:
            continue
        LOG.error("%s download failures (%d):", label, len(bucket))
        for path, msg in bucket[:25]:
            LOG.error("  %s -> %s", path, msg)
        if len(bucket) > 25:
            LOG.error("  ... and %d more", len(bucket) - 25)

    # Catastrophic only when nothing at all came through despite attempts — that
    # signals broken auth/config, not a rate-limit hiccup. A partial pull is fine:
    # plots render from cache + whatever was fetched, and leftover files retry next run.
    if failed and fetched == 0:
        LOG.error("no files could be downloaded; failing job")
        return 1
    if failed:
        LOG.warning(
            "%d file(s) could not be downloaded this run; continuing with a "
            "partial snapshot (will retry on the next run)",
            failed,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
