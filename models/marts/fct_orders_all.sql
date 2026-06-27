{{ config(materialized='table') }}

with horseflex as (

    select
        cast(order_id as nvarchar(255))  as order_id,
        order_nummer,
        order_datum,
        order_status,
        land_code,
        btw_land,
        bruto_bedrag,
        netto_bedrag,
        verzendkosten,
        btw_totaal,
        korting_totaal,
        cast(klant_id as nvarchar(255))  as klant_id,
        null                             as bedrijfsnaam,
        order_type,
        'Horseflex'                      as source
    from {{ ref('fct_orders_horseflex') }}

),

resellers as (

    select
        cast(order_nummer as nvarchar(255)) as order_id,
        order_nummer,
        order_datum,
        order_status,
        land_code,
        null                             as btw_land,
        net_bedrag                       as bruto_bedrag,
        net_bedrag                       as netto_bedrag,
        null                             as verzendkosten,
        null                             as btw_totaal,
        null                             as korting_totaal,
        null                             as klant_id,
        bedrijfsnaam,
        'reseller'                       as order_type,
        'Reseller'                       as source
    from {{ ref('fct_orders_resellers') }}

),

unioned as (

    select * from horseflex
    union all
    select * from resellers

)

select * from unioned
