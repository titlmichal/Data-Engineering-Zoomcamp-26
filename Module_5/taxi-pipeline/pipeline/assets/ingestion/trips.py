"""@bruin

name: ingestion.trips  # schema.asset_name
type: python
image: python:3.11     # Bruin runs Python in an isolated Python image
connection: duckdb-default

materialization:
  type: table          # write tabular data returned by materialize() into DuckDB
  strategy: append     # raw ingestion is append-only; duplicates handled in staging

@bruin"""

import json
import os
from datetime import datetime, date
from typing import Dict, Iterable, List

import pandas as pd
from dateutil.relativedelta import relativedelta


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def _month_starts(start: date, end: date) -> Iterable[date]:
    """
    Yield the first day of each month between start and end (inclusive).

    Simple helper so the main logic reads as:
    "for each month in the requested window..."
    """
    current = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)

    while current <= last:
        yield current
        current = current + relativedelta(months=1)


def _get_taxi_types_from_vars(bruin_vars: str) -> List[str]:
    """
    Parse BRUIN_VARS JSON and return the taxi_types list.

    Falls back to ["yellow"] so the asset is still usable
    even if no vars are explicitly passed.
    """
    if not bruin_vars:
        return ["yellow"]

    try:
        vars_dict: Dict[str, object] = json.loads(bruin_vars)
    except json.JSONDecodeError:
        # If vars are misconfigured, default to yellow and let the run proceed.
        return ["yellow"]

    taxi_types = vars_dict.get("taxi_types") or ["yellow"]
    # Ensure we always return a list of strings.
    if isinstance(taxi_types, str):
        return [taxi_types]
    return list(taxi_types)

def _strip_datetime_timezones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all datetime columns to ISO-format strings.
    PyArrow/dlt on Windows fails to resolve 'UTC' during upload to DuckDB;
    strings bypass timezone handling entirely. Staging can cast back to timestamp.
    """
    result = df.copy()
    for col in result.columns:
        if pd.api.types.is_datetime64_any_dtype(result[col]):
            result[col] = pd.to_datetime(result[col]).astype(str)
    return result


def _get_max_months_from_vars(bruin_vars: str) -> int | None:
    """
    Parse BRUIN_VARS JSON and return max_months (int or None for no limit).
    Falls back to a default (e.g. 3) if missing.
    """
    if not bruin_vars:
        return 3 # default value
    try:
        vars_dict = json.loads(bruin_vars)
    except json.JSONDecodeError:
        return 3
    val = vars_dict.get("max_months")
    if val is None:
        return 3
    return int(val)  # ensure it's an int even if JSON gave a float


def materialize() -> pd.DataFrame:
    """
    Ingest raw NYC taxi trip data for the current Bruin date window.

    Bruin provides:
    - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD) for the run window
    - BRUIN_VARS JSON containing pipeline variables (here: taxi_types)

    This function:
    - Figures out which months fall in the requested window
    - Builds the TLC parquet URLs for each (taxi_type, month) pair
    - Reads each parquet file into a DataFrame (no cleaning here)
    - Adds lightweight metadata columns (taxi_type, extracted_at)
    - Concatenates everything and returns a single DataFrame
    """
    start_str = os.environ.get("BRUIN_START_DATE")
    end_str = os.environ.get("BRUIN_END_DATE")
    max_months = _get_max_months_from_vars(os.environ.get("BRUIN_VARS", ""))

    if not start_str or not end_str:
        raise RuntimeError(
            "BRUIN_START_DATE and BRUIN_END_DATE environment variables must be set."
        )

    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()

    taxi_types = _get_taxi_types_from_vars(os.environ.get("BRUIN_VARS", ""))

    frames: List[pd.DataFrame] = []
    counter = 0

    for taxi_type in taxi_types:
        for month_start in _month_starts(start_date, end_date):
            file_name = f"{taxi_type}_tripdata_{month_start.year}-{month_start.month:02d}.parquet"
            url = f"{BASE_URL}{file_name}"

            # NOTE: We keep ingestion "raw" here – just load the parquet as-is.
            # Any cleaning, casting, or deduplication happens downstream in staging.
            df = pd.read_parquet(url)

            # Helpful metadata for debugging and lineage.
            # Use datetime.now() (naive, local) to avoid utcnow deprecation + tzdb issues on Windows.
            df["taxi_type"] = taxi_type
            df["extracted_at"] = datetime.now()

            frames.append(df)
            counter += 1
            if counter >= max_months:
                break

    if not frames:
        # If the date window produced no files, return an empty DataFrame.
        # With append strategy this simply results in "no new rows" for this run.
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    # Convert datetime cols to strings so PyArrow/DuckDB avoid tzdb lookup on Windows.
    return _strip_datetime_timezones(combined)