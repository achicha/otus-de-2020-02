with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_offense_codes') }}

),

deduplicated as (

    {#-
        we need only one name per code. remove others.
    #}
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

    {#-
        cast and rename fields
    #}
    select
        code::INTEGER as offense_code_id,
        name as offense_code_name
    from deduplicated

)

select * from renamed
