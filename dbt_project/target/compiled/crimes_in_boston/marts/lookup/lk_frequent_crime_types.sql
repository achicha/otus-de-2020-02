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
),crime_types as (
    select * from "postgres"."dbt"."lk_crime_type"
),

crimes as (
    select * from __dbt__CTE__crimes_joined
),

cnt_crime_type_by_district as (
    select
        t1.district
        ,t2.crime_type
        ,count(*) as cnt
        ,row_number() over (PARTITION BY district ORDER BY count(*) desc) as rn
    from crimes t1
        join crime_types t2 on t1.offense_code_id = t2.offense_code_id
    group by t1.district, t2.crime_type
),

final as (
    select
        district
        , array_agg(crime_type) as frequent_crime_types
    from cnt_crime_type_by_district
    where rn <= 3
    group by 1
)


select * from final