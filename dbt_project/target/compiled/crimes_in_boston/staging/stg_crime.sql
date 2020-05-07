with source as (
    select * from "postgres"."dbt"."raw_crime"

),

renamed as (
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
select distinct * from renamed