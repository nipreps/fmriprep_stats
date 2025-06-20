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

# Require credentials via environment variables
: "${DBNAME:?Set DBNAME, e.g., export DBNAME=your_db}"
: "${MONGO_USER:?Set MONGO_USER, e.g., export MONGO_USER=username}"
: "${MONGO_PASS:?Set MONGO_PASS, e.g., export MONGO_PASS=password}"

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

# Dump the database
mongodump --db "$DBNAME" \
    --username "$MONGO_USER" --password "$MONGO_PASS" \
    --out "$BACKUP_PATH"

# Stop mongod if we started it
if [ "$started_mongod" = true ]; then
    echo "Stopping mongod..."
    if command -v systemctl >/dev/null; then
        sudo systemctl stop mongod
    else
        sudo service mongod stop
    fi
fi
