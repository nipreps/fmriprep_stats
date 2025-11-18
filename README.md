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

The repository provides `scripts/migrate_mongo_to_parquet.py` to export
collections from MongoDB into Parquet files. Each issue collection is streamed
in batches and normalized with the same helpers used by the fetching CLI. Rows
that are missing identifiers or timestamps are skipped, while valid records are
written as `<dataset-root>/<event>/<YYYY-MM-DD>.parquet`. A manifest file named
`_manifest.parquet` is updated atomically so re-running the script is
idempotent. Example usage:

```bash
python scripts/migrate_mongo_to_parquet.py \
  --mongo-uri mongodb://localhost:27017 \
  --db fmriprep_stats \
  --dataset-root /data/fmriprep-parquet \
  --start-date 2022-01-01 --end-date 2022-02-01
```

Use the `--collections` flag to focus on a subset of issues while debugging,
and adjust `--batch-size` (default: 1000) if you need to reduce memory pressure
or better utilize fast disks. Streaming ensures only a day’s worth of data is
held in memory, but exporting a large history still requires several gigabytes
of temporary disk space for Parquet files.

After the first migration you can re-run the script with the same arguments to
confirm idempotency—the manifest ensures previously written partitions are
skipped so duplicates are avoided.

## Updating plots from Parquet exports

Once a Parquet dataset exists you can generate plots without MongoDB. Either
use `src/run.py plot --dataset-root /path/to/dataset` directly or run the
`scripts/update_plots_parquet.sh` helper, which mirrors `update_plots.sh` but
injects the dataset path:

```bash
chmod +x scripts/update_plots_parquet.sh
scripts/update_plots_parquet.sh /data/fmriprep-parquet
```

This script accepts an optional repository URL (defaults to
`git@github.com:nipreps/nipreps.github.io.git`) and otherwise behaves like the
original weekly plot updater.

