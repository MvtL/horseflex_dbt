{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('azure_landing', 'landing_flexrex_orders') }}

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
        cast(id as bigint)               as bestelling_id,
        cast(number as nvarchar(255))    as bestelnummer,
        order_key                        as bestelling_sleutel,
        cast(parent_id as bigint)        as hoofd_bestelling_id,
        cast(customer_id as bigint)      as klant_id,

        -- order context
        shop_url                         as webshop_url,
        status                           as bestelstatus,
        currency                         as valuta,
        created_via                      as aangemaakt_via,

        -- datums
        cast(date_created_gmt as date)      as besteldatum,
        cast(date_created_gmt as datetime2) as aangemaakt_op,
        cast(date_modified_gmt as datetime2) as gewijzigd_op,
        cast(date_paid_gmt as datetime2)    as betaald_op,
        cast(date_completed_gmt as datetime2) as afgerond_op,

        -- financieel
        try_cast(total          as decimal(18,4)) as bestelbedrag,
        try_cast(total_tax      as decimal(18,4)) as btw_totaal,
        try_cast(cart_tax       as decimal(18,4)) as winkelwagen_btw,
        try_cast(discount_total as decimal(18,4)) as korting_totaal,
        try_cast(discount_tax   as decimal(18,4)) as korting_btw,
        try_cast(shipping_total as decimal(18,4)) as verzendkosten,
        try_cast(shipping_tax   as decimal(18,4)) as verzendkosten_btw,

        -- betaling
        payment_method                       as betaalmethode,
        payment_method_title                 as betaalmethode_omschrijving,
        cast(transaction_id as nvarchar(255)) as transactie_id,

        -- vlaggen
        try_cast(prices_include_tax as bit)  as prijzen_incl_btw,
        try_cast(set_paid as bit)            as is_betaald,

        -- factuuradres
        json_value(cast(billing as nvarchar(max)), '$.first_name')  as factuur_voornaam,
        json_value(cast(billing as nvarchar(max)), '$.last_name')   as factuur_achternaam,
        json_value(cast(billing as nvarchar(max)), '$.email')       as factuur_email,
        json_value(cast(billing as nvarchar(max)), '$.phone')       as factuur_telefoon,
        json_value(cast(billing as nvarchar(max)), '$.address_1')   as factuur_straat,
        json_value(cast(billing as nvarchar(max)), '$.city')        as factuur_stad,
        json_value(cast(billing as nvarchar(max)), '$.postcode')    as factuur_postcode,
        json_value(cast(billing as nvarchar(max)), '$.country')     as factuur_land,

        -- verzendadres
        json_value(cast(shipping as nvarchar(max)), '$.first_name') as verzend_voornaam,
        json_value(cast(shipping as nvarchar(max)), '$.last_name')  as verzend_achternaam,
        json_value(cast(shipping as nvarchar(max)), '$.address_1')  as verzend_straat,
        json_value(cast(shipping as nvarchar(max)), '$.city')       as verzend_stad,
        json_value(cast(shipping as nvarchar(max)), '$.postcode')   as verzend_postcode,
        json_value(cast(shipping as nvarchar(max)), '$.country')    as verzend_land,

        -- btw
        json_value(cast(tax_lines as nvarchar(max)), '$[0].rate_code')       as btw_code,
        json_value(cast(tax_lines as nvarchar(max)), '$[0].label')           as btw_label,
        try_cast(json_value(cast(tax_lines as nvarchar(max)), '$[0].rate_percent') as decimal(5,2)) as btw_percentage,

        -- verzendmethode
        json_value(cast(shipping_lines as nvarchar(max)), '$[0].method_id')    as verzendmethode_code,
        json_value(cast(shipping_lines as nvarchar(max)), '$[0].method_title') as verzendmethode,

        -- overig
        customer_note                        as klant_opmerking,

        -- airbyte metadata
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id

    from deduplicated

)

select *
from cleaned
