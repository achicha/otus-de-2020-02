



select count(*)
from (

    select
        concat(incident_number, offense_code_id)

    from "postgres"."dbt"."stg_crime"
    where concat(incident_number, offense_code_id) is not null
    group by concat(incident_number, offense_code_id)
    having count(*) > 1

) validation_errors

