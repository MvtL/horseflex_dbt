{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('azure_landing', 'landing_horseflex_Goedgepikt_reseller_orders') }}

),

cleaned as (

    select
        -- identifiers
        _airbyte_raw_id                                          as order_line_id,
        cast(Description as nvarchar(255))                       as order_nummer,
        cast([Supplier item code] as nvarchar(255))              as product_code,

        -- datum
        cast(Datum as date)                                      as order_datum,

        -- klant
        cast(Voornaam as nvarchar(255))                          as voornaam,
        cast(Achternaam as nvarchar(255))                        as achternaam,
        cast(Bedrijfsnaam as nvarchar(255))                      as bedrijfsnaam,
        cast([E-mailadres] as nvarchar(255))                     as email,

        -- adres
        cast(Straat as nvarchar(255))                            as straat,
        cast(Huisnummer as nvarchar(50))                         as huisnummer,
        cast(Toevoeging as nvarchar(50))                         as toevoeging,
        cast(Postcode as nvarchar(20))                           as postcode,
        cast(Plaats as nvarchar(255))                            as stad,
        cast(Land as nvarchar(10))                               as land_code,

        -- financieel
        try_cast(Price as decimal(18,4))                         as verkoopsprijs,
        try_cast([Quantity ordered] as int)                      as aantal,
        try_cast(Price as decimal(18,4))
            * try_cast([Quantity ordered] as decimal(18,4))      as net_bedrag,

        -- bestandsmetadata
        _ab_source_file_url,
        _ab_source_file_last_modified,

        -- airbyte metadata
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id

    from source

)

select * from cleaned
