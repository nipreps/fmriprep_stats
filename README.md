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

`scripts/backup_mongodb.sh` dumps a MongoDB database to a Dropbox-synced
folder. The script starts `mongod` if it is not running and stops it again
when the backup finishes (if it was started by the script).

Make it executable before scheduling it with `cron`:

```bash
chmod +x scripts/backup_mongodb.sh
```
