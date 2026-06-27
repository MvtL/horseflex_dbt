{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('azure_landing', 'landing_horseflex_customers') }}

),

deduplicated as (

    select * from (
        select *,
            row_number() over (
                partition by id
                order by date_modified_gmt desc, _airbyte_extracted_at desc
            ) as _rn
        from source
    ) t
    where _rn = 1

),

cleaned as (

    select
        -- identifiers
        cast(id as bigint)                    as klant_id,
        cast(username as nvarchar(255))        as gebruikersnaam,
        role                                   as klant_rol,

        -- naam
        first_name                             as voornaam,
        last_name                              as achternaam,

        -- datums
        cast(date_created_gmt as datetime2)    as aangemaakt_op,
        cast(date_modified_gmt as datetime2)   as gewijzigd_op,

        -- vlaggen
        try_cast(is_paying_customer as bit)    as is_betalende_klant,

        -- factuuradres (uit billing JSON)
        json_value(cast(billing as nvarchar(max)), '$.email')      as klant_email,
        json_value(cast(billing as nvarchar(max)), '$.phone')      as klant_telefoon,
        json_value(cast(billing as nvarchar(max)), '$.address_1')  as straat,
        json_value(cast(billing as nvarchar(max)), '$.city')       as stad,
        json_value(cast(billing as nvarchar(max)), '$.postcode')   as postcode,
        json_value(cast(billing as nvarchar(max)), '$.country')    as land_code,

        -- airbyte metadata
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id

    from deduplicated

)

select *
from cleaned
