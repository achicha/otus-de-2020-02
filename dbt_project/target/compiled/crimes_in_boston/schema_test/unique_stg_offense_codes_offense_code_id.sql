



select count(*)
from (

    select
        offense_code_id

    from "postgres"."dbt"."stg_offense_codes"
    where offense_code_id is not null
    group by offense_code_id
    having count(*) > 1

) validation_errors

