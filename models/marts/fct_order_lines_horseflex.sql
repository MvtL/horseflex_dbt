{{ config(materialized='table') }}

with orders as (

    select
        order_id                                                 as bestelling_id,
        klant_id,
        order_datum,
        order_status,
        land_code,
        valuta,
        regel_items
    from {{ ref('fct_orders_horseflex') }}
    where regel_items is not null

),

order_lines as (

    select
        o.bestelling_id                              as order_id,
        o.klant_id,                                  -- FK → dim_klanten
        o.order_datum,
        o.order_status,
        o.land_code,
        o.valuta,

        -- regel details (uitgebreid via OPENJSON)
        try_cast(li.regel_id as bigint)              as regel_id,
        li.productnaam,
        try_cast(li.product_id as bigint)            as product_id,
        try_cast(li.variatie_id as bigint)           as variatie_id,
        li.sku                                       as artikelnummer,
        try_cast(li.aantal as int)                   as aantal,
        try_cast(li.prijs as decimal(18,4))          as stukprijs,
        try_cast(li.subtotaal as decimal(18,4))      as subtotaal,
        try_cast(li.subtotaal_btw as decimal(18,4))  as subtotaal_btw,
        try_cast(li.totaal as decimal(18,4))         as regel_totaal,
        try_cast(li.totaal_btw as decimal(18,4))     as regel_btw,
        try_cast(li.totaal as decimal(18,4))
            - try_cast(li.subtotaal as decimal(18,4)) as korting_bedrag

    from orders o
    cross apply openjson(o.regel_items)
    with (
        regel_id      nvarchar(50)    '$.id',
        productnaam   nvarchar(500)   '$.name',
        product_id    nvarchar(50)    '$.product_id',
        variatie_id   nvarchar(50)    '$.variation_id',
        sku           nvarchar(255)   '$.sku',
        aantal        nvarchar(50)    '$.quantity',
        prijs         nvarchar(50)    '$.price',
        subtotaal     nvarchar(50)    '$.subtotal',
        subtotaal_btw nvarchar(50)    '$.subtotal_tax',
        totaal        nvarchar(50)    '$.total',
        totaal_btw    nvarchar(50)    '$.total_tax'
    ) as li

)

select *
from order_lines
