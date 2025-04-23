

with join_location as 
(
select
*
from
{{ ref("fct_activity_log")}}

left outer join 

{{ref("locations")}}

on 

position_lat between min_lat and max_lat 
and
position_long between min_lon and max_lon
)

select * from join_location