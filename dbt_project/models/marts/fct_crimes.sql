with crimes as (

    select * from {{ ref('crimes_joined') }}

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
