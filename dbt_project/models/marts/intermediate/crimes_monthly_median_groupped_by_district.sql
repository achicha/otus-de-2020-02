{#- transform to with_statement (CTE) #}
{{
  config(
    materialized = "ephemeral",
  )
}}

with crimes as (

    select * from {{ ref('crimes_joined') }}

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
