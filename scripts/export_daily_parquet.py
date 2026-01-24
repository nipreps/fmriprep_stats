#!/usr/bin/env python3
"""Stub exporter for CI workflows.

Validates MongoDB connectivity and accepts the CLI arguments used in CI.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pymongo import MongoClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stub daily parquet exporter for CI validation."
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Destination directory for parquet output.",
    )
    parser.add_argument(
        "--events",
        nargs="+",
        required=True,
        help="Event names to export.",
    )
    parser.add_argument(
        "--num-days",
        type=int,
        required=True,
        help="Number of days to export.",
    )
    parser.add_argument(
        "--mongo-uri",
        default="mongodb://localhost:27017",
        help="MongoDB connection URI.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    client.list_database_names()


if __name__ == "__main__":
    main()
