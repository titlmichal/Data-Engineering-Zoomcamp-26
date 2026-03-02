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

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: Trip pickup timestamp.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Trip dropoff timestamp.
    nullable: false
    checks:
      - name: not_null
  - name: pu_location_id
    type: integer
    description: Pickup location id.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: do_location_id
    type: integer
    description: Dropoff location id.
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: fare_amount
    type: double
    description: Fare amount in USD.
    primary_key: true
    checks:
      - name: non_negative
  - name: payment_type
    type: integer
    description: Raw payment type id.
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Payment type label from lookup seed.
  - name: taxi_type
    type: string
    description: Taxi category (yellow/green).
    checks:
      - name: not_null
  - name: passenger_count
    type: double
    description: Number of passengers.
    checks:
      - name: non_negative
  - name: trip_distance
    type: double
    description: Trip distance in miles.
    checks:
      - name: non_negative
  - name: total_amount
    type: double
    description: Total charged amount in USD.
    checks:
      - name: non_negative
  - name: extracted_at
    type: timestamp
    description: Ingestion extraction timestamp.
    checks:
      - name: not_null
  - name: vendor_id
    type: integer
    description: Vendor identifier.
    checks:
      - name: non_negative

custom_checks:
  - name: row_count_positive
    description: Ensure the staged interval has rows.
    query: |
      SELECT COUNT(*) > 0
      FROM staging.trips
      WHERE pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
    value: 1

@bruin */

WITH source_data AS (
  SELECT
    COALESCE(
      TRY_CAST(tpep_pickup_datetime AS TIMESTAMP),
      TRY_CAST(lpep_pickup_datetime AS TIMESTAMP)
    ) AS pickup_datetime,
    COALESCE(
      TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP),
      TRY_CAST(lpep_dropoff_datetime AS TIMESTAMP)
    ) AS dropoff_datetime,
    TRY_CAST(pu_location_id AS INTEGER) AS pu_location_id,
    TRY_CAST(do_location_id AS INTEGER) AS do_location_id,
    TRY_CAST(fare_amount AS DOUBLE) AS fare_amount,
    TRY_CAST(payment_type AS INTEGER) AS payment_type,
    taxi_type,
    TRY_CAST(passenger_count AS DOUBLE) AS passenger_count,
    TRY_CAST(trip_distance AS DOUBLE) AS trip_distance,
    TRY_CAST(total_amount AS DOUBLE) AS total_amount,
    TRY_CAST(extracted_at AS TIMESTAMP) AS extracted_at,
    TRY_CAST(vendor_id AS INTEGER) AS vendor_id
  FROM ingestion.trips
  WHERE COALESCE(
          TRY_CAST(tpep_pickup_datetime AS TIMESTAMP),
          TRY_CAST(lpep_pickup_datetime AS TIMESTAMP)
        ) >= '{{ start_datetime }}'
    AND COALESCE(
          TRY_CAST(tpep_pickup_datetime AS TIMESTAMP),
          TRY_CAST(lpep_pickup_datetime AS TIMESTAMP)
        ) < '{{ end_datetime }}'
),
filtered_data AS (
  SELECT *
  FROM source_data
  WHERE pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND pu_location_id IS NOT NULL
    AND do_location_id IS NOT NULL
    AND payment_type IS NOT NULL
    AND fare_amount >= 0
    AND passenger_count >= 0
    AND trip_distance >= 0
    AND total_amount >= 0
),
deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, fare_amount, taxi_type
      ORDER BY extracted_at DESC
    ) AS rn
  FROM filtered_data
)
SELECT
  d.pickup_datetime,
  d.dropoff_datetime,
  d.pu_location_id,
  d.do_location_id,
  d.fare_amount,
  d.payment_type,
  p.payment_type_name,
  d.taxi_type,
  d.passenger_count,
  d.trip_distance,
  d.total_amount,
  d.extracted_at,
  d.vendor_id
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
  ON d.payment_type = p.payment_type_id
WHERE d.rn = 1
