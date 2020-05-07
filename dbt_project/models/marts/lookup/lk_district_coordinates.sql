with crimes as (

    select * from {{ ref('crimes_joined') }}

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
