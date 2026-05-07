# Legacy scripts

Tools that are still callable but no longer part of the active flow.
Kept for archival reference and for one-off re-runs (parity checks, ad-hoc
re-exports) during the long tail of the MongoDB → parquet migration.

| Script | Origin | Status |
|---|---|---|
| `backup_mongodb.sh` | nightly mongodump → Dropbox (local cron) | retired with the Mongo source-of-truth |
| `export_daily_parquet.py` | one-time MongoDB → daily-parquet backfill | exercised by `.github/workflows/export-daily-parquet.yml` (kept as a regression smoke test) |
| `parity_check_daily_parquet.py` | row-count parity between Mongo and parquet | useful when re-running an `export_daily_parquet.py` backfill window |

All require `FMRIPREP_STATS_ENABLE_MONGO=1` (or implicit Mongo access) to run;
they are not invoked by any production workflow other than the export-daily-parquet
CI smoke test.

The `--store mongo` codepath in `src/run.py` and the Mongo helpers in
`src/db.py` are not in this directory — they remain feature-flagged behind
`FMRIPREP_STATS_ENABLE_MONGO`. See `MIGRATION_RUNBOOK.md` for the deprecation
checkpoints.
