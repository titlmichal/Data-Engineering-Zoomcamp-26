/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips
type: duckdb.sql
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# there is ton of possibilities for materialization strategies, e.g.:
# - create+replace (full rebuild)
# - truncate+insert (full refresh without drop/create)
# - append (insert new rows only)
# - delete+insert (refresh partitions based on incremental_key values)
# - merge (upsert based on primary key)
# - time_interval (refresh rows within a time window)

materialization:
  type: table # recreate table every time its ran

# also, can set column definition and checks on them here too

# these are custom ones
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: row_count_positive
    description: Checks if the row count is positive, true = 1
    query: |
      SELECT COUNT(*) > 0 FROM ingestion.trips
    value: 1

@bruin */

-- Staging: clean, deduplicate, enrich with payment lookup.
-- Ingestion stores datetimes as ISO strings (Windows PyArrow workaround); we cast back to TIMESTAMP.
-- Filter by {{ start_datetime }}/{{ end_datetime }} so time_interval only inserts the run window.

-- Yellow taxi: tpep_*; green taxi: lpep_*. Default is yellow-only so we use tpep_*.
-- If you add green, ensure ingestion concat produces both cols, then use COALESCE(tpep_*, lpep_*).
WITH base AS (
  SELECT
    TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    COALESCE(pu_location_id, 0) AS pu_location_id,
    COALESCE(do_location_id, 0) AS do_location_id,
    fare_amount,
    payment_type,
    taxi_type,
    TRY_CAST(extracted_at AS TIMESTAMP) AS extracted_at,
    passenger_count,
    trip_distance,
    total_amount,
    ratecode_id,
    vendor_id
  FROM ingestion.trips
),
deduped AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, fare_amount
      ORDER BY extracted_at DESC NULLS LAST
    ) AS rn
  FROM base
  WHERE pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
)
SELECT
  d.pickup_datetime,
  d.dropoff_datetime,
  d.pu_location_id,
  d.do_location_id,
  d.fare_amount,
  d.payment_type,
  pl.payment_type_name,
  d.taxi_type,
  d.passenger_count,
  d.trip_distance,
  d.total_amount,
  d.ratecode_id,
  d.vendor_id
FROM deduped d
LEFT JOIN ingestion.payment_lookup pl ON d.payment_type = pl.payment_type_id
WHERE d.rn = 1
  AND d.pickup_datetime >= '{{ start_datetime }}'
  AND d.pickup_datetime < '{{ end_datetime }}'
