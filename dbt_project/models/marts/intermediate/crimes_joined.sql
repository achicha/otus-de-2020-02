{#- transform to with_statement (CTE) #}
{{
  config(
    materialized = "ephemeral",
  )
}}

with crime as (

    select * from {{ ref('stg_crime') }}

),

offense_codes as (

    select * from {{ ref('stg_offense_codes') }}

),

final as (
    select
        c.*
        ,oc.offense_code_name
    from crime c
    join offense_codes oc on c.offense_code_id = oc.offense_code_id
)

select * from final
