with source as (
    select * from "postgres"."dbt"."raw_offense_codes"

),

deduplicated as (
    select tt.code, tt.name
    from (
        select
            code
            ,name
            ,row_number() over (PARTITION BY code ORDER BY name) as rn
        from source
    ) tt
    where rn = 1

),

renamed as (
    select
        code::INTEGER as offense_code_id,
        name as offense_code_name
    from deduplicated

)

select * from renamed