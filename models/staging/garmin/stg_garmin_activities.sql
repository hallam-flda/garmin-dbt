

-- simple query that selects relevant columns
-- should be as close to the source data as possible
-- might change data types
-- simple transformations (i.e. meters to km)

with staging_query as (
        select
            cadence,
            distance as distance_meters,
            ROUND(distance/1000,2) distance_km,
            ROUND(enhanced_altitude,0) as altitude,
            ROUND(enhanced_speed,2) as speed,
            heart_rate,
            position_lat * (180/2147483648) position_lat,
            position_long * (180/2147483648) position_long,
            CAST(timestamp AS TIMESTAMP) timestamp
        from 
            {{ source('garmin', 'activities') }}

        WHERE
            position_lat is not null and position_long is not null
    )

    select * from staging_query