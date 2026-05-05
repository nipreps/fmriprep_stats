# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Utilities to download fMRIPrep usage statistics from Sentry.io, persist them, and generate longitudinal plots. The project is mid-migration from MongoDB-as-source-of-truth to parquet snapshots; both code paths still exist in parallel.

## Common commands

```bash
# Install
python -m pip install -r requirements.txt

# Fetch events from Sentry (requires SENTRY_TOKEN). Default --store is parquet.
SENTRY_TOKEN=... python src/run.py get -m started -m success -S 2023-01-01 -E 2023-01-31

# Generate plots from parquet snapshots in a directory
python src/run.py plot --source parquet --parquet-dir /path/to/parquet

# Generate from both sources (Mongo + parquet) and compare weekly counts
FMRIPREP_STATS_ENABLE_MONGO=1 python src/run.py plot --source both --parquet-dir /path --compare-sources

# Backfill Mongo → parquet for a deterministic window
python scripts/export_daily_parquet.py --start-date 2024-01-01 --end-date 2024-01-08 --output-dir /path

# Validate parity for one day/event (non-zero exit on mismatch)
python scripts/parity_check_daily_parquet.py --event success --day 2024-01-01 --output-dir /path

# Lint / format (no pre-commit; run manually)
ruff check --fix
ruff format
```

There are no automated tests in this repo.

## Architecture

The code is split into three layers, all under `src/`:

- **`src/api.py`** — Sentry HTTP fetcher. `parallel_fetch()` chunks a date range into windows (default 1 day) and dispatches them across a `ThreadPoolExecutor`. `ISSUES` maps human-readable event names (`started`, `success`, `failed`, `no_disk`, `sigkill`) to Sentry issue IDs. `normalize_events()` flattens Sentry tags into top-level columns.
- **`src/db.py`** — Persistence + post-fetch normalization. MongoDB access is gated behind `FMRIPREP_STATS_ENABLE_MONGO=1`; calling any Mongo helper without it raises. `normalize_event_frame()` is shared by both backends — it parses `dateCreated`, derives `date_minus_time`, and dedupes on `run_uuid`. `massage_versions()` collapses fMRIPrep versions to `MAJOR.MINOR` buckets and folds pre-21.0 into `older`.
- **`src/data_sources.py`** — Source-agnostic loader. `load_event(event, source=...)` dispatches to either Mongo (`db.load_event`) or parquet (reads all `*-<event>.parquet` files in a directory via `pyarrow.dataset`). Plotting code consumes only this interface.
- **`src/viz.py`** — `plot_performance()` (weekly benchmark) and `plot_version_stream()`.
- **`src/run.py`** — Click CLI exposing `get` and `plot`.

### Import convention

`src/run.py` imports siblings with bare names (`from api import ...`, `from db import ...`) — it expects to be invoked as `python src/run.py`, not as a package. By contrast, `scripts/*.py` insert the repo root into `sys.path` and import `from src.api import ISSUES`. Don't mix the two styles.

### Parquet file convention

One file per day per event, named `YYYY-MM-DD-<event>.parquet`. The date in the filename is the *last complete day* in the fetched window (i.e., `end_date - 1 day`). `data_sources.load_event_parquet()` globs `*-<event>.parquet` and concatenates — so files from any number of daily snapshots in one directory compose into a single dataframe.

### Mongo → parquet migration

See `MIGRATION_RUNBOOK.md`. The scheme: backfill oldest→newest in deterministic UTC windows, validate each day/event with `parity_check_daily_parquet.py`, cut consumers over to parquet only after a full validation window passes, then disable Mongo writes. The `FMRIPREP_STATS_ENABLE_MONGO` flag is the kill switch for downstream consumers — keep new code parquet-first and only touch `db.py` Mongo paths if a Mongo-specific path is unavoidable.

## CI/CD (GitHub Actions)

- **`sentry-fetch-daily.yml`** — daily at 23:59 UTC. Fetches yesterday's `started`/`success`/`failed` events, writes `output/YYYY-MM-DD-<event>.parquet`, uploads as artifact and to Dropbox.
- **`sentry-fetch-pr.yml`** — runs on PRs, fetches one day of `failed` as a smoke test.
- **`update-plots.yml`** — Mondays at 22:00 UTC. Hydrates parquet from cache → prior-run artifact (`update-plots.yml` named `parquet-cache`) → Dropbox, generates plots, uploads to Dropbox.
- **`export-daily-parquet.yml`** — runs on changes to the export script; spins up MongoDB, restores a test dump from a secret URL, exercises `export_daily_parquet.py`.

Dropbox credentials (`DROPBOX_APP_KEY`/`SECRET`/`REFRESH_TOKEN`) and `SENTRY_TOKEN` are repo secrets.

## Conventions

- **Commit messages**: Conventional Commits (https://www.conventionalcommits.org/en/v1.0.0/).
- **PR titles**: Conventional Commits with the type in **upper case** and **no scope** (e.g. `FIX: handle empty windows`, not `fix(api): ...`).
- **Planning**: AGENTS.md asks for plan-first work — think through critical points (especially anything touching the Mongo/parquet boundary) before editing.
