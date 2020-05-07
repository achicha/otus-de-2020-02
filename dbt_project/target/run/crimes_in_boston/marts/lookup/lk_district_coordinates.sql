

  create  table "postgres"."dbt"."lk_district_coordinates__dbt_tmp"
  as (
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
    select
        district
        ,round(avg(latitude), 5) as latitude
        ,round(avg(longitude), 5) as longitude
    from crimes
    group by 1
)

select * from final
  );