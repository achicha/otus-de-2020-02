


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

    select district, incident_number from __dbt__CTE__crimes_joined

),

final as (
    select
        district
        ,count(incident_number) as crimes_total
    from crimes
    group by 1
)

select * from final