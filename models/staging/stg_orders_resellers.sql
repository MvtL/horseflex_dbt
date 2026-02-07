with source as (

    select *
    from {{ source('airbyte_landing', 'Goedgepikt_reseller_orders') }}

),

renamed as (

    select
        _airbyte_raw_id                       as order_id,
        Land                                  as land_code,
        Price                                 as net_verkoopprijs_1,
        --Quantity_ordered * Price              as net_bedrag,
    --  Plaats,
    --  Straat,
    --  Postcode,
    --  Voornaam,
    --  Achternaam,
    --  Huisnummer,
    --  Toevoeging,
        cast(Description as string)           as order_nummer,
        date(Datum)                           as order_datum,
    --   E_mailadres,
        Bedrijfsnaam,
        Quantity_ordered,
        Supplier_item_code,
        _ab_source_file_url,
        _ab_source_file_last_modified

    from source
),

pricelist as (
    -- Zorg dat dit 1 regel per join-key is (simpel gehouden met max)
    select
        SKU,
        Verkoopprijs_ex_btw
    from {{ ref('stg_prijslijst') }}
    group by 1,2
),

final as (
    select
        r.order_id,
        r.order_nummer,
        r.order_datum,
        r.land_code,
        coalesce(r.net_verkoopprijs_1, p.Verkoopprijs_ex_btw) as net_verkoopsprijs,
        coalesce(r.net_verkoopprijs_1, p.Verkoopprijs_ex_btw) * Quantity_ordered as net_bedrag,
        r.Bedrijfsnaam,
        r.Quantity_ordered,
        r.Supplier_item_code,
        r._ab_source_file_url,
        r._ab_source_file_last_modified
    from renamed r
    left join pricelist p
      on r.Supplier_item_code = p.SKU
)

select * from final