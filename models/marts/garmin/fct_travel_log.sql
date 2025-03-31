WITH
  base_table AS (
    SELECT
      *,
      LEAD(timestamp) OVER (PARTITION BY activity_id ORDER BY timestamp ASC) lead_timestamp,
      LEAD(position_lat) OVER (PARTITION BY activity_id ORDER BY timestamp ASC) lead_lat,
      LEAD(position_long) OVER (PARTITION BY activity_id ORDER BY timestamp ASC) lead_long
    FROM {{ ref('int_activities_clean') }}
  ),

  distance_and_time_diffs AS (
    SELECT
      *,
      ST_DISTANCE(
        ST_GEOGPOINT(position_long, position_lat),
        ST_GEOGPOINT(lead_long, lead_lat)
      ) / 1000 AS dist_to_next_row_km
    FROM base_table
  )

SELECT 
activity_id,
position_lat,
position_long,
lead_lat,
lead_long

FROM distance_and_time_diffs
WHERE dist_to_next_row_km > 30
