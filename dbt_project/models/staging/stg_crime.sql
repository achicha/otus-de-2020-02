with source as (

    {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
    select * from {{ ref('raw_crime') }}

),

renamed as (

    {#-
        1. cast and rename fields
        2. remove crimes without district
    #}
    select
        INCIDENT_NUMBER as incident_number
        ,OFFENSE_CODE::INTEGER as offense_code_id
        ,DISTRICT as district
        ,OCCURRED_ON_DATE::TIMESTAMP as incident_date
        ,YEAR::INTEGER as year
        ,MONTH::INTEGER as month
        ,Lat::DECIMAL as latitude
        ,Long::DECIMAL as longitude
    from source
    where district is not null

)

{#-
    1. distinct by INCIDENT_NUMBER
#}
select distinct * from renamed
