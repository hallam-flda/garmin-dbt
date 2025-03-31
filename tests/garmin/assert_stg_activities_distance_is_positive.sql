with activities as (select * from {{ ref("stg_garmin_activities") }})

select 
timestamp,
min(distance_km) min_distance

from
activities

group by 
timestamp

having
min_distance < 0 

