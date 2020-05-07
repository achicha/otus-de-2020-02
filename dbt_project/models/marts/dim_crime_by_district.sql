with crimes_total as (
    select * from {{ ref('crimes_total_groupped_by_district') }}
),

crimes_monthly_median as (
    select * from {{ ref('crimes_monthly_median_groupped_by_district') }}
),

district_coordinates as (
    select * from {{ ref('lk_district_coordinates') }}
),

frequent_crime_types as (
    select * from {{ ref('lk_frequent_crime_types') }}
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
