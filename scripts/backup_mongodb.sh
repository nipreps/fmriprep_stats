#!/bin/bash
# scripts/backup_mongodb.sh
# Backup MongoDB database, ensuring mongod is running.

set -euo pipefail

# Optionally source credentials from ~/.mongodb_backup_env
ENV_FILE="$HOME/.mongodb_backup_env"
if [ -f "$ENV_FILE" ]; then
    # shellcheck disable=SC1090
    source "$ENV_FILE"
fi

# DBNAME is required; credentials are optional
: "${DBNAME:?Set DBNAME, e.g., export DBNAME=your_db}"

DATE=$(date +%Y-%m-%d)
BACKUP_DIR="$HOME/Dropbox/backups"
BACKUP_PATH="$BACKUP_DIR/db_backup_${DATE}"

mkdir -p "$BACKUP_DIR"

# Track whether we started mongod
started_mongod=false

# Check if mongod process is running
if pgrep mongod >/dev/null; then
    echo "mongod is running"
else
    echo "mongod is not running. Starting..."
    if command -v systemctl >/dev/null; then
        sudo systemctl start mongod
    else
        sudo service mongod start
    fi
    started_mongod=true
fi

# Build mongodump options
dump_opts=(--db "$DBNAME" --out "$BACKUP_PATH")
if [ -n "${MONGO_USER:-}" ]; then
    dump_opts+=(--username "$MONGO_USER")
fi
if [ -n "${MONGO_PASS:-}" ]; then
    dump_opts+=(--password "$MONGO_PASS")
fi

# Dump the database
mongodump "${dump_opts[@]}"

# Stop mongod if we started it
if [ "$started_mongod" = true ]; then
    echo "Stopping mongod..."
    if command -v systemctl >/dev/null; then
        sudo systemctl stop mongod
    else
        sudo service mongod stop
    fi
fi
