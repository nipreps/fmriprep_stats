#!/bin/bash
# scripts/backup_mongodb.sh
# Backup MongoDB database, ensuring mongod is running.

DATE=$(date +%Y-%m-%d)
BACKUP_DIR="$HOME/Dropbox/backups"
DBNAME="DBNAME"
USERNAME="USERNAME"
PASSWORD="PASSWORD"
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
mongodump --db "$DBNAME" --username "$USERNAME" --password "$PASSWORD" --out "$BACKUP_PATH"

# Stop mongod if we started it
if [ "$started_mongod" = true ]; then
    echo "Stopping mongod..."
    if command -v systemctl >/dev/null; then
        sudo systemctl stop mongod
    else
        sudo service mongod stop
    fi
fi
