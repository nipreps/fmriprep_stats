# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
"""Database helpers used by the CLI and plotting utilities."""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Tuple, Union

import pandas as pd
from pymongo import MongoClient


def _prepare_dataframe(data: pd.DataFrame, unique: bool = True) -> pd.DataFrame:
    if "dateCreated" in data.columns:
        data["dateCreated"] = pd.to_datetime(data["dateCreated"])
    if "dateCreated" in data.columns:
        data["date_minus_time"] = data["dateCreated"].apply(
            lambda df: datetime.datetime(year=df.year, month=df.month, day=df.day)
        )
    if unique and "run_uuid" in data.columns:
        data = data.drop_duplicates(subset=["run_uuid"])
    return data


def load_event(event_name: str, unique: bool = True) -> pd.DataFrame:
    """Load one event collection from MongoDB."""
    db = MongoClient().fmriprep_stats
    data = pd.DataFrame(list(db[event_name].find()))
    if len(data) == 0:
        raise RuntimeError(f"No records of event '{event_name}'")
    return _prepare_dataframe(data, unique=unique)


def load_event_from_parquet(
    dataset_root: Union[str, Path], event_name: str, unique: bool = True
) -> pd.DataFrame:
    """Load event records stored as Parquet files."""

    dataset_root = Path(dataset_root)
    event_dir = dataset_root / event_name
    if not event_dir.exists():
        raise RuntimeError(f"No Parquet export found for '{event_name}' in {dataset_root}")

    files = sorted(event_dir.glob("*.parquet"))
    if not files:
        raise RuntimeError(
            f"No Parquet files found for '{event_name}' under {event_dir}"
        )

    frames = [pd.read_parquet(path) for path in files]
    data = pd.concat(frames, ignore_index=True)
    return _prepare_dataframe(data, unique=unique)


def massage_versions(
    started: pd.DataFrame, success: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize version strings as done in the analysis notebook."""
    started = started.copy()
    success = success.copy()

    started = started.fillna(value={"environment_version": "older"})
    success = success.fillna(value={"environment_version": "older"})

    started.loc[started.environment_version == "v0.0.1", "environment_version"] = "older"
    success.loc[success.environment_version == "v0.0.1", "environment_version"] = "older"

    started.loc[started.environment_version.str.startswith("20.0"), "environment_version"] = "older"
    success.loc[success.environment_version.str.startswith("20.0"), "environment_version"] = "older"
    started.loc[started.environment_version.str.startswith("20.1"), "environment_version"] = "older"
    success.loc[success.environment_version.str.startswith("20.1"), "environment_version"] = "older"

    versions = sorted(
        {
            ".".join(v.split(".")[:2])
            for v in started.environment_version.unique()
            if "." in str(v)
        }
    )
    for ver in versions:
        started.loc[started.environment_version.str.startswith(ver), "environment_version"] = ver
        success.loc[success.environment_version.str.startswith(ver), "environment_version"] = ver

    return started, success
