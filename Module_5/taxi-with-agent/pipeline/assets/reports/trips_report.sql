/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: trip_date
  time_granularity: date

columns:
  - name: trip_date
    type: date
    description: Pickup date.
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Taxi category.
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type
    type: integer
    description: Payment type id.
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Payment type label.
  - name: trip_count
    type: bigint
    description: Number of trips.
    checks:
      - name: non_negative
  - name: total_passengers
    type: double
    description: Total passengers.
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: double
    description: Sum of fares.
    checks:
      - name: non_negative
  - name: total_trip_distance
    type: double
    description: Sum of trip distance.
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: double
    description: Average fare per trip.
    checks:
      - name: non_negative

@bruin */

SELECT
  CAST(pickup_datetime AS DATE) AS trip_date,
  taxi_type,
  payment_type,
  COALESCE(payment_type_name, 'unknown') AS payment_type_name,
  COUNT(*) AS trip_count,
  SUM(passenger_count) AS total_passengers,
  SUM(fare_amount) AS total_fare_amount,
  SUM(trip_distance) AS total_trip_distance,
  AVG(fare_amount) AS avg_fare_amount
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  taxi_type,
  payment_type,
  COALESCE(payment_type_name, 'unknown')
