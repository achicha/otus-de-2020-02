



select count(*)
from (

    select
        district

    from "postgres"."dbt"."dim_crime_by_district"
    where district is not null
    group by district
    having count(*) > 1

) validation_errors

