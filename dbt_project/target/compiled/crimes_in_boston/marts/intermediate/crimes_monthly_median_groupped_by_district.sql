


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

crimes_monthly_raw as (
    select
        district
        ,year
        ,month
        ,count(incident_number) as crimes_total
    from crimes
    group by 1,2,3
),

final as (
    select
        district
        ,ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY crimes_total)::numeric, 0) AS crimes_monthly
    from crimes_monthly_raw
    group by 1
)

select * from final