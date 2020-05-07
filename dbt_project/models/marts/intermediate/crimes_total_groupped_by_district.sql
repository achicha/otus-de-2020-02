{#- transform to with_statement (CTE) #}
{{
  config(
    materialized = "ephemeral",
  )
}}

with crimes as (

    select district, incident_number from {{ ref('crimes_joined') }}

),

final as (
    select
        district
        ,count(incident_number) as crimes_total
    from crimes
    group by 1
)

select * from final
