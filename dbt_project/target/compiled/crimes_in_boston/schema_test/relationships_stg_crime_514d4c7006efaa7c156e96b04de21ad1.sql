




select count(*)
from (
    select offense_code_id as id from "postgres"."dbt"."stg_crime"
) as child
left join (
    select offense_code_id as id from "postgres"."dbt"."stg_offense_codes"
) as parent on parent.id = child.id
where child.id is not null
  and parent.id is null

