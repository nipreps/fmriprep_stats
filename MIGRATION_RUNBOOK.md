# Parquet Migration Runbook

## Backfill schedule

1. Backfill oldest → newest to avoid rework when new data arrives.
2. Use deterministic windows per batch (for example, weekly ranges) with
   `scripts/export_daily_parquet.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD`
   so the same window can be re-run safely.
3. Keep exports in UTC unless there is a clear business reason to choose a
   different timezone, since day boundaries are timezone-sensitive.

## Validation steps

1. For each day/event exported, run the parity check:

   ```bash
   python scripts/parity_check_daily_parquet.py --event <event> --day YYYY-MM-DD --output-dir <parquet_dir>
   ```

2. Investigate and re-export on any non-zero exit code.
3. Track parity results in a checklist so every day/event is validated before
   moving on.

## Cut-over strategy

1. Define a verification window (for example, N days) that must pass parity
   checks for all events.
2. Update consumers to read parquet only after all days in the window are
   validated.
3. Continue to write MongoDB data during the validation window to allow fast
   re-exports.

## Deprecation checkpoint

Once parity checks pass for a full retention window (e.g., the full range of
days consumers care about), proceed with the deprecation steps:

1. Disable MongoDB readers in downstream consumers.
2. Stop writing new MongoDB data for events that are fully backed by parquet.
3. Keep MongoDB backups for a defined grace period before final archival or
   removal.
