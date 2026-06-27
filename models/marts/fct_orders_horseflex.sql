{{ config(materialized='table') }}

with orders as (

    select
        -- identifiers
        bestelling_id                                           as order_id,
        bestelnummer                                            as order_nummer,
        bestelling_sleutel                                      as order_sleutel,
        hoofd_bestelling_id,
        klant_id,                                               -- FK → dim_klanten

        -- order context
        webshop_url,
        bestelstatus                                            as order_status,
        valuta,
        aangemaakt_via,

        -- datums
        besteldatum                                             as order_datum,
        aangemaakt_op,
        gewijzigd_op,
        betaald_op,
        afgerond_op,

        -- financieel
        bestelbedrag                                            as bruto_bedrag,
        bestelbedrag - btw_totaal                               as netto_bedrag,
        btw_totaal,
        winkelwagen_btw,
        korting_totaal,
        korting_btw,
        verzendkosten,
        verzendkosten_btw,

        -- btw info
        btw_code,
        btw_land,
        btw_label,
        btw_percentage,

        -- betaling
        betaalmethode,
        betaalmethode_omschrijving,
        transactie_id,
        is_betaald,
        prijzen_incl_btw,

        -- verzending
        verzendmethode_code,
        verzendmethode,

        -- overig (orderspecifiek)
        order_type,
        factuur_land                                            as land_code,
        klant_opmerking,

        -- json (voor order lines)
        regel_items,

        -- airbyte metadata
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id

    from {{ ref('stg_orders_horseflex') }}

)

select *
from orders
