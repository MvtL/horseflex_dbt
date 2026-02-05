with source as (

    select *
    from {{ source('airbyte_landing', 'Goedgepikt_reseller_orders') }}

),

renamed as (

    select
        _airbyte_raw_id                       as order_id,
        Land                                  as land_code,
        Price                                 as net_bedrag,
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
)

select *
from renamed