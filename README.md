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

Example usage:

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
python src/run.py plot
```

will generate the performance and version stream plots using the records stored
in MongoDB.

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
temporary clone.  You may pass an alternative repository URL as an argument.
The script may be run from any directory.

Make the script executable:

```bash
chmod +x scripts/update_plots.sh
```

To run it every Monday at 5 AM, add this line to your crontab:

```
0 5 * * 1 /path/to/fmriprep_stats/scripts/update_plots.sh 2>> $HOME/var/log/update_plots.err >> $HOME/var/log/update_plots.log
```

## Migrating from MongoDB

`scripts/migrate_mongo_to_parquet.py` streams the MongoDB collections into the
partitioned Parquet layout consumed by the new tooling. Run it before switching
workflows so that `_manifest.parquet` already knows which events have been
ingested:

```bash
python scripts/migrate_mongo_to_parquet.py \
  --mongo-uri mongodb://localhost:27017 \
  --db-name fmriprep_stats \
  /path/to/dataset
```

Events are grouped by calendar day (the default) so that each
`date=YYYY-MM-DD` directory contains a single part file named
`part-YYYY-MM-DD_<hash>.parquet`. Pass `--partition-frequency week` if you
prefer larger weekly files such as `week=2024-03-04/part-2024-W10_<hash>.parquet`.

The script buffers up to 1,000 events before spilling to a temporary directory
inside the dataset root. At the end of the run it rewrites each affected Parquet
file once, so choose a smaller `--batch-size` if you are memory-constrained or a
larger size when working with SSD-backed storage. Re-running the migration is
safe: the manifest tracks event IDs and stops duplicates—we tested a
double-run and the second invocation reported no new rows.

