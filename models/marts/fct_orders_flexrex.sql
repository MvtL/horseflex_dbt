{{ config(materialized='table') }}

with flexrex as (

    select
        cast(bestelling_id as nvarchar(255))     as order_id,
        bestelnummer                             as order_nummer,
        besteldatum                              as order_datum,
        factuur_land                             as land_code,
        'FlexRex klant'                          as klant_rol,
        bestelbedrag - btw_totaal                as net_bedrag,
        'FlexRex'                                as source
    from {{ ref('stg_orders_flexrex') }}

)

select * from flexrex
