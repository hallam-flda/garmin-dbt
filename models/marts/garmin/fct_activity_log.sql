

WITH
  base_table AS (
  SELECT
    *,
    MAX(timestamp) OVER (PARTITION BY activity_id) end_time,
    MIN(timestamp) OVER (PARTITION BY activity_id) start_time,
    LEAD(timestamp) OVER (PARTITION BY activity_id ORDER BY timestamp ASC) lead_timestamp,
    LEAD(position_lat) OVER (PARTITION BY activity_id ORDER BY timestamp ASC) lead_lat,
    LEAD(position_long) OVER (PARTITION BY activity_id ORDER BY timestamp ASC) lead_long,
    MAX(distance_meters) OVER (PARTITION BY activity_id) end_distance_meters,
    MIN(distance_meters) OVER (PARTITION BY activity_id) start_distance_meters
  FROM
    {{ ref('int_activities_clean') }}
     )

, distance_and_time_diffs as 
(  
SELECT
  *,
  TIMESTAMP_DIFF(lead_timestamp, timestamp, SECOND) time_increment,
  -- to remove the row where activities lapse
  ST_DISTANCE( ST_GEOGPOINT(position_long, position_lat), ST_GEOGPOINT(lead_long, lead_lat) ) / 1000 dist_to_next_row_km
FROM
  base_table
)

, activity_time as 
(
    SELECT
    * EXCEPT (time_increment),
    sum(time_increment) over (partition by activity_id) activity_duration_seconds

    from
    distance_and_time_diffs

    where 
    dist_to_next_row_km < 1

)

select
*,
ROUND(activity_duration_seconds/60,2) activity_duration_minutes,
ROUND(end_distance_meters/activity_duration_seconds,2) avg_speed_ms,
ROUND(60 / ((end_distance_meters/activity_duration_seconds)*3.6),2) pace_mins_per_km

from
activity_time

order by activity_id asc, timestamp asc
