



select count(*)
from "postgres"."dbt"."stg_offense_codes"
where offense_code_id is null

