#!/usr/bin/env python3
"""List ISO weeks with zero events for each tracked event type.

Reads the parquet directory produced by the daily Sentry-fetch action,
groups events by ISO (year, week), and reports every Monday-anchored week
between the earliest and latest event for which the loaded DataFrame has
no rows. Prints a deterministic, line-oriented summary so it can be
diff'd across runs and pasted into PR descriptions.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from data_sources import load_event_parquet  # noqa: E402


def expected_weekly_index(start: pd.Timestamp, end: pd.Timestamp) -> pd.MultiIndex:
    mondays = pd.date_range(
        start - pd.Timedelta(days=int(start.weekday())),
        end,
        freq="W-MON",
    )
    iso = mondays.isocalendar()
    return pd.MultiIndex.from_arrays(
        [iso.year.astype("int64"), iso.week.astype("int64")],
        names=["year", "week"],
    )


def audit(parquet_dir: str, events=("started", "success", "failed")) -> int:
    rc = 0
    for ev in events:
        df = load_event_parquet(ev, parquet_dir, unique=True)
        iso = df["date_minus_time"].dt.isocalendar()
        observed = pd.MultiIndex.from_arrays(
            [iso["year"].astype("int64").values, iso["week"].astype("int64").values],
            names=["year", "week"],
        ).unique()
        full = expected_weekly_index(
            df["date_minus_time"].min(), df["date_minus_time"].max()
        )
        missing = full.difference(observed)
        print(
            f"[{ev}] expected_weeks={len(full)} observed_weeks={len(observed)} "
            f"missing={len(missing)} "
            f"range={df['date_minus_time'].min().date()}..{df['date_minus_time'].max().date()}"
        )
        for y, w in sorted(missing):
            iso_monday = pd.Timestamp.fromisocalendar(int(y), int(w), 1).date()
            print(f"  {ev}  {int(y)}-W{int(w):02d}  (week starting {iso_monday})")
        if len(missing):
            rc = 1
    return rc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet-dir", required=True, help="Directory of *-<event>.parquet files")
    args = parser.parse_args()
    return audit(args.parquet_dir)


if __name__ == "__main__":
    raise SystemExit(main())
