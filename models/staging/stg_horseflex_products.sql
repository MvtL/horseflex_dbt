{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('azure_landing', 'landing_horseflex_products') }}

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
        cast(id as bigint)               as product_id,
        cast(parent_id as bigint)        as hoofd_product_id,
        cast(sku as nvarchar(255))       as artikelnummer,
        cast(slug as nvarchar(255))      as url_naam,

        -- product info
        cast(name as nvarchar(500))      as productnaam,
        cast(type as nvarchar(100))      as product_type,
        cast(status as nvarchar(100))    as product_status,
        cast(catalog_visibility as nvarchar(100)) as catalogus_zichtbaarheid,
        cast(description as nvarchar(max))       as beschrijving,
        cast(short_description as nvarchar(max)) as korte_beschrijving,
        cast(permalink as nvarchar(2000))        as permalink,
        cast(button_text as nvarchar(500))       as knoptekst,
        cast(purchase_note as nvarchar(max))     as aankoopnotitie,
        cast(external_url as nvarchar(2000))     as externe_url,

        -- prijzen
        try_cast(price as decimal(18,4))         as prijs,
        try_cast(regular_price as decimal(18,4)) as reguliere_prijs,
        try_cast(sale_price as decimal(18,4))    as actieprijs,
        cast(price_html as nvarchar(max))        as prijs_html,

        -- btw & verzending
        cast(tax_status as nvarchar(100))        as btw_status,
        cast(tax_class as nvarchar(100))         as btw_klasse,
        try_cast(shipping_required as bit)       as verzending_verplicht,
        try_cast(shipping_taxable as bit)        as verzending_btw_plichtig,
        cast(shipping_class as nvarchar(255))    as verzendklasse,
        cast(shipping_class_id as bigint)        as verzendklasse_id,

        -- voorraad
        try_cast(manage_stock as bit)            as voorraad_beheren,
        cast(stock_status as nvarchar(100))      as voorraadstatus,
        try_cast(stock_quantity as int)          as voorraadaantal,
        try_cast(backordered as bit)             as is_nabesteld,
        cast(backorders as nvarchar(100))        as nabestellingen,
        try_cast(backorders_allowed as bit)      as nabestellingen_toegestaan,

        -- vlaggen
        try_cast(on_sale as bit)                 as in_actie,
        try_cast(virtual as bit)                 as is_virtueel,
        try_cast(downloadable as bit)            as is_downloadbaar,
        try_cast(purchasable as bit)             as is_koopbaar,
        try_cast(reviews_allowed as bit)         as beoordelingen_toegestaan,
        try_cast(sold_individually as bit)       as afzonderlijk_verkopen,

        -- statistieken
        try_cast(total_sales as bigint)          as totaal_verkopen,
        try_cast(rating_count as int)            as aantal_beoordelingen,
        try_cast(average_rating as decimal(5,2)) as gemiddelde_beoordeling,

        -- afmetingen & gewicht
        cast(weight as nvarchar(50))             as gewicht,
        cast(dimensions as nvarchar(max))        as afmetingen,

        -- downloads
        try_cast(download_limit as int)          as download_limiet,
        try_cast(download_expiry as int)         as download_verloop,

        -- volgorde
        try_cast(menu_order as int)              as menu_volgorde,

        -- datums
        cast(date_created_gmt as datetime2)      as aangemaakt_op,
        cast(date_modified_gmt as datetime2)     as gewijzigd_op,
        cast(date_on_sale_from_gmt as datetime2) as actie_start_op,
        cast(date_on_sale_to_gmt as datetime2)   as actie_eind_op,

        -- json velden
        cast(categories as nvarchar(max))        as categorieen,
        cast(tags as nvarchar(max))              as tags,
        cast(images as nvarchar(max))            as afbeeldingen,
        cast(attributes as nvarchar(max))        as attributen,
        cast(default_attributes as nvarchar(max)) as standaard_attributen,
        cast(variations as nvarchar(max))        as variaties,
        cast(upsell_ids as nvarchar(max))        as upsell_ids,
        cast(cross_sell_ids as nvarchar(max))    as cross_sell_ids,
        cast(related_ids as nvarchar(max))       as gerelateerde_ids,
        cast(grouped_products as nvarchar(max))  as gegroepeerde_producten,
        cast(features as nvarchar(max))          as kenmerken,
        cast(downloads as nvarchar(max))         as downloads,
        cast(meta_data as nvarchar(max))         as meta_data,

        -- airbyte metadata
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id

    from deduplicated

)

select *
from cleaned
