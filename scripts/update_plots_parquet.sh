#!/bin/bash
# scripts/update_plots_parquet.sh
# Generate plots from Parquet exports and push them to nipreps.github.io.

set -euo pipefail

DATASET_ROOT=${1:?"Usage: $0 <dataset-root> [repo-url]"}
REPO_URL="${2:-git@github.com:nipreps/nipreps.github.io.git}"
TMP_REPO="$(mktemp -d)"

cleanup() {
    rm -rf "$TMP_REPO"
}
trap cleanup EXIT

git clone "$REPO_URL" "$TMP_REPO"

ASSETS_DIR="$TMP_REPO/docs/assets"
mkdir -p "$ASSETS_DIR"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

"$( which conda )" run -n base src/run.py plot --dataset-root "$DATASET_ROOT" -o "$ASSETS_DIR"

cd "$TMP_REPO"
git pull --ff-only
git add docs/assets
if ! git diff --cached --quiet; then
    git commit -m "Update stats plots from Parquet"
    git push
else
    echo "No changes to commit."
fi
