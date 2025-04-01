with
    base_table as (
        select
            *,
            rank() over (partition by activity_id order by timestamp asc) activity_step
        from {{ ref("fct_activity_log") }}
        order by activity_id asc, timestamp asc
    ),
    percentage_ranking as (
        select
            *,
            percent_rank() over (
                partition by activity_id order by activity_step
            ) percent_step
        from base_table
    ),
    flag_keep_rows as (
        select
            *,
            case
                when percent_step <= 0.05 or percent_step >= 0.95
                then 'Delete'
                when mod(activity_step, 5) = 0
                then 'Keep'
                else 'Delete'
            end
            keep_criteria
        from percentage_ranking
    ),
    condensed_rows as (
        select * except (keep_criteria, lead_lat, lead_long) from flag_keep_rows where keep_criteria = 'Keep'
    )

    -- have to reassess any large gaps between activities.

    ,lead_coords as (
        select
            *,
            max(timestamp) over (partition by activity_id) max_timestamp,
            lead(position_lat) over (
                partition by activity_id order by timestamp asc
            ) lead_lat,
            lead(position_long) over (
                partition by activity_id order by timestamp asc
            ) lead_long
        from condensed_rows
    )
select
    activity_id,
    heart_rate,
    cadence,
    altitude,
    distance_km,
    position_lat,
    position_long,
    round(end_distance_meters/1000,2) end_distance_km,
    timestamp,
    activity_duration_seconds,
    activity_duration_minutes,
    avg_speed_ms,
    pace_mins_per_km,
    max_timestamp,
    lead_lat,
    lead_long,
    st_distance(
        st_geogpoint(position_long, position_lat), st_geogpoint(lead_long, lead_lat)
    )
    / 1000 dist_to_next_act

from lead_coords

where
    -- drop rows that jump large distances, this happens when either I forget to
    -- record or GPS is lost but looks strange on map.
    st_distance(
        st_geogpoint(position_long, position_lat), st_geogpoint(lead_long, lead_lat)
    )
    / 1000
    < 1
