{{ config(materialized='table') }}

with producten as (

    select
        -- identifiers
        product_id,                         -- PK
        hoofd_product_id,
        artikelnummer,
        url_naam,

        -- product info
        productnaam,
        product_type,
        product_status,
        catalogus_zichtbaarheid,
        beschrijving,
        korte_beschrijving,
        permalink,

        -- prijzen
        prijs,
        reguliere_prijs,
        actieprijs,
        actie_start_op,
        actie_eind_op,

        -- btw & verzending
        btw_status,
        btw_klasse,
        verzending_verplicht,
        verzending_btw_plichtig,
        verzendklasse,
        verzendklasse_id,

        -- voorraad
        voorraad_beheren,
        voorraadstatus,
        voorraadaantal,
        is_nabesteld,
        nabestellingen,
        nabestellingen_toegestaan,

        -- vlaggen
        in_actie,
        is_virtueel,
        is_downloadbaar,
        is_koopbaar,
        beoordelingen_toegestaan,
        afzonderlijk_verkopen,

        -- statistieken
        totaal_verkopen,
        aantal_beoordelingen,
        gemiddelde_beoordeling,

        -- afmetingen & gewicht
        gewicht,
        afmetingen,

        -- categorieën & tags (json)
        categorieen,
        tags,

        -- datums
        aangemaakt_op,
        gewijzigd_op

    from {{ ref('stg_horseflex_products') }}

)

select *
from producten
