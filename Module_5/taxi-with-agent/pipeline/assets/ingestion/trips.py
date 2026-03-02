"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: taxi_type
    type: string
    description: Taxi type for the source file (yellow or green).
  - name: extracted_at
    type: timestamp
    description: Timestamp when this row was fetched by the ingestion asset.

@bruin"""

import json
import os
from io import BytesIO
from datetime import datetime, date
from typing import Iterable, List

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
ALLOWED_TAXI_TYPES = {"yellow", "green"}
EXPECTED_DATETIME_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
]


def _month_starts(start: date, end: date) -> Iterable[date]:
    current = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while current <= last:
        yield current
        current = current + relativedelta(months=1)


def _read_taxi_types() -> List[str]:
    raw = os.environ.get("BRUIN_VARS", "")
    if not raw:
        return ["yellow"]

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return ["yellow"]

    taxi_types = parsed.get("taxi_types", ["yellow"])
    if isinstance(taxi_types, str):
        taxi_types = [taxi_types]

    cleaned = [str(t).strip().lower() for t in taxi_types if str(t).strip()]
    valid = [t for t in cleaned if t in ALLOWED_TAXI_TYPES]
    return valid or ["yellow"]


def _strip_datetime_timezones(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in result.columns:
        if pd.api.types.is_datetime64_any_dtype(result[col]):
            result[col] = pd.to_datetime(result[col]).astype(str)
    return result


def _ensure_expected_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for col in EXPECTED_DATETIME_COLUMNS:
        if col not in result.columns:
            # Keep as empty string so the column is still materialized by dlt.
            result[col] = ""
    return result


def materialize() -> pd.DataFrame:
    start_str = os.environ.get("BRUIN_START_DATE")
    end_str = os.environ.get("BRUIN_END_DATE")
    if not start_str or not end_str:
        raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be set.")

    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
    taxi_types = _read_taxi_types()

    frames: List[pd.DataFrame] = []

    for taxi_type in taxi_types:
        for month_start in _month_starts(start_date, end_date):
            file_name = f"{taxi_type}_tripdata_{month_start.year}-{month_start.month:02d}.parquet"
            url = f"{BASE_URL}{file_name}"
            try:
                response = requests.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=120,
                )
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                df = pd.read_parquet(BytesIO(response.content))
            except Exception:
                # Missing months/files are expected for some ranges; skip them.
                continue
            df = _ensure_expected_columns(df)
            df["taxi_type"] = taxi_type
            df["extracted_at"] = datetime.utcnow()
            frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["taxi_type", "extracted_at"])

    combined = pd.concat(frames, ignore_index=True)
    return _strip_datetime_timezones(combined)
