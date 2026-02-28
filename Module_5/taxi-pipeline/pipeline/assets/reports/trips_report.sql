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

columns:
  - name: trip_date
    type: date
    description: Date of the trip (pickup date)
    primary_key: true
  - name: taxi_type
    type: string
    description: Yellow or green taxi
    primary_key: true
  - name: payment_type
    type: integer
    description: Payment type ID
    primary_key: true
  - name: payment_type_name
    type: string
    description: Human-readable payment type
  - name: trip_count
    type: bigint
    description: Number of trips
    checks:
      - name: non_negative
  - name: total_passengers
    type: double
    description: Sum of passenger_count
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: double
    description: Sum of fare_amount in USD
    checks:
      - name: non_negative
  - name: total_trip_distance
    type: double
    description: Sum of trip_distance in miles
    checks:
      - name: non_negative

@bruin */

-- Aggregate staging trips by date, taxi_type, payment_type for dashboards.
-- Filter by {{ start_datetime }}/{{ end_datetime }} for time_interval consistency.

SELECT
  CAST(pickup_datetime AS DATE) AS trip_date,
  taxi_type,
  payment_type,
  payment_type_name,
  COUNT(*) AS trip_count,
  SUM(passenger_count) AS total_passengers,
  SUM(fare_amount) AS total_fare_amount,
  SUM(trip_distance) AS total_trip_distance
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  taxi_type,
  payment_type,
  payment_type_name
