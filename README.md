# fmriprep_stats

This repository contains utilities to download usage statistics of fMRIPrep from Sentry.io.

## Setup

1. Install Python 3.8 or newer.
2. Install dependencies using `requirements.txt`:

   ```bash
   python -m pip install -r requirements.txt
   ```

## Running `src/run.py`

`src/run.py` exposes a command line interface built with Click. Before running,
set a `SENTRY_TOKEN` environment variable with a valid token.

Example usage (defaults to parquet output unless `--store mongo` is provided):

```bash
SENTRY_TOKEN=your_token python src/run.py get -m started -m success -s 2023-01-01 -u 2023-01-31
```

This will fetch the "started" and "success" events for January 2023. Omit the
`-s` and `-u` options to use the default window (last 90 days). Run

```bash
python src/run.py --help
```

for a description of all available options.

Running:

```bash
python src/run.py plot --parquet-dir /path/to/parquet/files
```

will generate the performance and version stream plots using parquet snapshots
stored in the provided directory (files named `YYYY-MM-DD-<event>.parquet`).

MongoDB access is now opt-in: set `FMRIPREP_STATS_ENABLE_MONGO=1` when you
explicitly need to read or write MongoDB data (for example, to compare sources
or perform parity checks during migration).

To generate plots from parquet snapshots stored in a single directory, pass
the source and parquet directory:

```bash
python src/run.py plot --source parquet --parquet-dir /path/to/parquet/files
```

To render plots from both sources in one run, use `--source both`. Output files
will include `_mongo` and `_parquet` suffixes to avoid overwriting:

```bash
python src/run.py plot --source both --parquet-dir /path/to/parquet/files
```

To compare weekly aggregate counts between MongoDB and parquet, add the
`--compare-sources` flag (this requires parquet access as well, plus
`FMRIPREP_STATS_ENABLE_MONGO=1`):

```bash
python src/run.py plot --source parquet --parquet-dir /path/to/parquet/files --compare-sources
```

## MongoDB backup script

`scripts/backup_mongodb.sh` dumps a MongoDB database and creates a compressed
`db_backup_YYYY-MM-DD.tar.gz` file in a Dropbox-synced folder. The script
starts `mongod` if it is not running and stops it again when the backup
finishes (if it was started by the script).

Make it executable before scheduling it with `cron`:

```bash
chmod +x scripts/backup_mongodb.sh
```

Store `DBNAME` (and optional credentials) in environment variables rather than
editing the script.  You may create a file named `~/.mongodb_backup_env` with
content like:

```bash
export DBNAME="fmriprep_stats"
# export MONGO_USER=myuser
# export MONGO_PASS=mypassword
```

The backup script will source this file if present.

## Weekly plot update script

`scripts/update_plots.sh` generates plots with `src/run.py plot` and pushes them
to the `nipreps.github.io` website. The script clones the repository to a
temporary directory (by default using `git@github.com:nipreps/nipreps.github.io.git`),
writes the plots there, commits and pushes the changes, and removes the
temporary clone.  You may pass an alternative repository URL as an argument and
the parquet directory as the second argument (or set `PARQUET_DIR`). The script
may be run from any directory.

Make the script executable:

```bash
chmod +x scripts/update_plots.sh
```

To run it every Monday at 5 AM, add this line to your crontab:

```
0 5 * * 1 /path/to/fmriprep_stats/scripts/update_plots.sh git@github.com:nipreps/nipreps.github.io.git /path/to/parquet 2>> $HOME/var/log/update_plots.err >> $HOME/var/log/update_plots.log
```

## Daily parquet exports and parity checks

`scripts/export_daily_parquet.py` exports MongoDB events into daily parquet files.
For deterministic backfills, supply `--start-date` and `--end-date` (both in
`YYYY-MM-DD` format). These dates are interpreted in the selected timezone
(default: UTC). When both start and end dates are provided, they take precedence
over `--num-days` so repeated backfills produce the same window.

`scripts/parity_check_daily_parquet.py` validates a single day/event by comparing
the MongoDB `dateCreated` count to the parquet row count for that file. It exits
with a non-zero status on mismatch.

When switching downstream consumers to parquet-only data, ensure you have
completed parity checks across the full retention window before disabling
MongoDB reads/writes to avoid silent gaps in reporting.
