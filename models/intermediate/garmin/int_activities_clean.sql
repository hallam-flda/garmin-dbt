
with activities as 
(
    select
    *,
    lag(timestamp,1) over (order by timestamp asc) lag_timestamp

    from
    {{ ref('stg_garmin_activities') }}

)

, flag_large_time_difference as 
(
    SELECT
    *,
    TIMESTAMP_DIFF(timestamp, lag_timestamp, HOUR) time_diff_hours,
    CASE 
      WHEN TIMESTAMP_DIFF(timestamp, lag_timestamp, HOUR) >= 6 THEN 1 
      ELSE 0 
    END AS activity_end_flag
  FROM activities
)

,session_flag as 
  (
  SELECT 
  *,
  sum(activity_end_flag) over (order by timestamp asc) activity_id
  FROM flag_large_time_difference 
  )

select
activity_id,
cadence,
distance_meters,
distance_km,
altitude,
speed,
heart_rate,
position_lat,
position_long,
timestamp

from session_flag

order by timestamp asc