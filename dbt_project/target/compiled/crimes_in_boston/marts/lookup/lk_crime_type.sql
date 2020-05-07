with offense_codes as (

    select * from "postgres"."dbt"."stg_offense_codes"

),

final as (
    select
        offense_code_id
        ,split_part(offense_code_name, ' ', 1) as crime_type
    from offense_codes
)

select * from final