with  __dbt__CTE__crimes_joined as (



with crime as (

    select * from "postgres"."dbt"."stg_crime"

),

offense_codes as (

    select * from "postgres"."dbt"."stg_offense_codes"

),

final as (
    select
        c.*
        ,oc.offense_code_name
    from crime c
    join offense_codes oc on c.offense_code_id = oc.offense_code_id
)

select * from final
),crimes as (

    select * from __dbt__CTE__crimes_joined

),

final as (
    select distinct
        incident_number
        ,incident_date
        ,ARRAY_AGG( offense_code_id ) as offense_codes
    from crimes
    group by 1,2
)

select * from final