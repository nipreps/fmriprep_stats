# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
"""Database helpers used by the CLI and plotting utilities."""

from __future__ import annotations

import datetime
import os
from typing import Callable, Iterable, Tuple

import pandas as pd
from pymongo import MongoClient

MONGO_FEATURE_FLAG = "FMRIPREP_STATS_ENABLE_MONGO"


def _mongo_enabled() -> bool:
    return os.getenv(MONGO_FEATURE_FLAG, "").lower() in {"1", "true", "yes", "on"}


def _require_mongo_enabled() -> None:
    if not _mongo_enabled():
        raise RuntimeError(
            "MongoDB access is disabled. Set "
            f"{MONGO_FEATURE_FLAG}=1 to enable MongoDB reads/writes."
        )


def normalize_event_frame(data: pd.DataFrame, unique: bool = True) -> pd.DataFrame:
    """Normalize event frames for plotting."""
    if len(data) == 0:
        raise RuntimeError("No records available for plotting.")

    data = data.copy()
    data["dateCreated"] = pd.to_datetime(data["dateCreated"])
    data["date_minus_time"] = data["dateCreated"].apply(
        lambda df: datetime.datetime(year=df.year, month=df.month, day=df.day)
    )
    if unique:
        data = data.drop_duplicates(subset=["run_uuid"])
    return data


def mongo_id_lookup(event_name: str) -> Callable[[Iterable[str]], set[str]]:
    """Return a lookup function for cached event ids."""
    _require_mongo_enabled()
    collection = MongoClient().fmriprep_stats[event_name]
    collection.create_index("id", unique=True)

    def _lookup(ids: Iterable[str]) -> set[str]:
        ids = list(ids)
        if not ids:
            return set()
        return set(collection.distinct("id", {"id": {"$in": ids}}))

    return _lookup


def store_events(event_name: str, records: pd.DataFrame | Iterable[dict]) -> int:
    """Persist fetched records to MongoDB."""
    _require_mongo_enabled()
    if isinstance(records, pd.DataFrame):
        docs = records.to_dict("records")
    else:
        docs = list(records)

    if not docs:
        return 0

    collection = MongoClient().fmriprep_stats[event_name]
    collection.create_index("id", unique=True)
    result = collection.insert_many(docs)
    return len(result.inserted_ids)


def load_event(event_name: str, unique: bool = True) -> pd.DataFrame:
    """Load one event collection from MongoDB."""
    _require_mongo_enabled()
    db = MongoClient().fmriprep_stats
    data = pd.DataFrame(list(db[event_name].find()))
    if len(data) == 0:
        raise RuntimeError(f"No records of event '{event_name}'")

    return normalize_event_frame(data, unique=unique)


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
