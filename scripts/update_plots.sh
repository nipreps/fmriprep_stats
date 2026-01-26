#!/bin/bash
# scripts/update_plots.sh
# Generate weekly plots and push them to the nipreps.github.io repository.

set -euo pipefail

REPO_URL="${1:-git@github.com:nipreps/nipreps.github.io.git}"
PARQUET_DIR="${2:-${PARQUET_DIR:-}}"
TMP_REPO="$(mktemp -d)"

cleanup() {
    rm -rf "$TMP_REPO"
}
trap cleanup EXIT

if [[ -z "$PARQUET_DIR" ]]; then
    echo "ERROR: PARQUET_DIR is required (pass as arg 2 or set PARQUET_DIR)." >&2
    exit 2
fi

PARQUET_DIR="$(cd "$PARQUET_DIR" && pwd)"
# Keep absolute path since we later cd into the repo root before plotting.

git clone "$REPO_URL" "$TMP_REPO"

ASSETS_DIR="$TMP_REPO/docs/assets"
mkdir -p "$ASSETS_DIR"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

$( which conda ) run -n base python src/run.py plot \
    --source parquet \
    --parquet-dir "$PARQUET_DIR" \
    -o "$ASSETS_DIR"

cd "$TMP_REPO"
git pull --ff-only
git add docs/assets
if ! git diff --cached --quiet; then
    git commit -m "Update stats plots"
    git push
else
    echo "No changes to commit."
fi
