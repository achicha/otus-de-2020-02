



select count(*)
from (

    select
        incident_number

    from "postgres"."dbt"."fct_crimes"
    where incident_number is not null
    group by incident_number
    having count(*) > 1

) validation_errors

