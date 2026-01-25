# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
"""Data access utilities for plotting."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow.dataset as ds

from db import load_event as load_event_mongo
from db import normalize_event_frame

DEFAULT_COLUMNS = ("id", "run_uuid", "dateCreated", "environment_version")


def _resolve_parquet_files(parquet_dir: str | Path, event_name: str) -> list[Path]:
    parquet_dir = Path(parquet_dir)
    if not parquet_dir.exists():
        raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")
    files = sorted(parquet_dir.glob(f"*-{event_name}.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No parquet files found for event '{event_name}' in {parquet_dir}"
        )
    return files


def load_event_parquet(
    event_name: str,
    parquet_dir: str | Path,
    unique: bool = True,
    columns: Iterable[str] = DEFAULT_COLUMNS,
) -> pd.DataFrame:
    """Load an event from parquet files in a directory."""
    files = _resolve_parquet_files(parquet_dir, event_name)
    dataset = ds.dataset([str(path) for path in files], format="parquet")
    table = dataset.to_table(columns=list(columns))
    data = table.to_pandas()
    return normalize_event_frame(data, unique=unique)


def load_event(
    event_name: str,
    source: str = "mongo",
    parquet_dir: str | Path | None = None,
    unique: bool = True,
    columns: Iterable[str] = DEFAULT_COLUMNS,
) -> pd.DataFrame:
    """Load event data from the requested source."""
    source = source.lower()
    if source == "mongo":
        return load_event_mongo(event_name, unique=unique)
    if source == "parquet":
        if parquet_dir is None:
            raise ValueError("parquet_dir is required when source='parquet'")
        return load_event_parquet(
            event_name, parquet_dir=parquet_dir, unique=unique, columns=columns
        )
    raise ValueError(f"Unknown source '{source}'")
