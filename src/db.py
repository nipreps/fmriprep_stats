# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
"""Database helpers used by the CLI and plotting utilities."""

from __future__ import annotations

import datetime
from typing import Tuple

import pandas as pd
from pymongo import MongoClient


def load_event(event_name: str, unique: bool = True) -> pd.DataFrame:
    """Load one event collection from MongoDB."""
    db = MongoClient().fmriprep_stats
    data = pd.DataFrame(list(db[event_name].find()))
    if len(data) == 0:
        raise RuntimeError(f"No records of event '{event_name}'")

    data["dateCreated"] = pd.to_datetime(data["dateCreated"])
    data["date_minus_time"] = data["dateCreated"].apply(
        lambda df: datetime.datetime(year=df.year, month=df.month, day=df.day)
    )
    if unique:
        data = data.drop_duplicates(subset=["run_uuid"])
    return data


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
