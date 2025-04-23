
with activities_clean as
(
 select * from {{ ref('fct_activity_log')}}
)


, monthly as 
(
    select
    -- get month from timestmap
    activity_id,
    EXTRACT(YEAR from timestamp) activity_year,
    EXTRACT(MONTH from timestamp) activity_month,
    activity_duration_minutes

    from
    activities_clean

    group BY
    activity_id,
    activity_year,
    activity_month,
    activity_duration_minutes
    
)

select
activity_year,
activity_month,
round(sum(activity_duration_minutes),0) monthly_active_minutes,
round(sum(activity_duration_minutes)/60,1) monthly_active_hours

from
monthly


group by 
activity_year,
activity_month

order by activity_year asc, activity_month asc