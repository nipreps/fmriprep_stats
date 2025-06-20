#!/bin/bash
# scripts/update_plots.sh
# Generate weekly plots and push them to the nipreps.github.io repository.

set -euo pipefail

REPO_DIR="${1:-$HOME/workspace/nipreps.github.io}"
ASSETS_DIR="$REPO_DIR/docs/assets"

if [ ! -d "$REPO_DIR/.git" ]; then
    echo "Error: $REPO_DIR is not a git repository" >&2
    exit 1
fi

mkdir -p "$ASSETS_DIR"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

python src/run.py plot -o "$ASSETS_DIR"

cd "$REPO_DIR"
git pull --ff-only
git add docs/assets
if ! git diff --cached --quiet; then
    git commit -m "Update stats plots"
    git push
else
    echo "No changes to commit."
fi

