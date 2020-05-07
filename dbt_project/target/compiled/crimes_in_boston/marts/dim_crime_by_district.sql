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
),  __dbt__CTE__crimes_total_groupped_by_district as (



with crimes as (

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
),  __dbt__CTE__crimes_monthly_median_groupped_by_district as (



with crimes as (

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
),crimes_total as (
    select * from __dbt__CTE__crimes_total_groupped_by_district
),

crimes_monthly_median as (
    select * from __dbt__CTE__crimes_monthly_median_groupped_by_district
),

district_coordinates as (
    select * from "postgres"."dbt"."lk_district_coordinates"
),

frequent_crime_types as (
    select * from "postgres"."dbt"."lk_frequent_crime_types"
),


final as (
    select
        t1.*
        ,t2.crimes_monthly
        ,t4.frequent_crime_types
        ,t3.latitude
        ,t3.longitude
    from crimes_total t1
        join crimes_monthly_median t2 on t1.district = t2.district
        join district_coordinates t3 on t1.district = t3.district
        join frequent_crime_types t4 on t1.district = t4.district
)

select * from final