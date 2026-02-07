with source as (

    select *
    from {{ source('airbyte_landing', 'Prijslijst') }}

),

renamed as (

    select
        _airbyte_raw_id                        as product_id,
        cast(EAN as string)                    as ean,
        SKU,
        Merk,
        Naam,
        Inhoud,
        Verpakking,
        Verkoopprijs_ex_btw                    as verkoopprijs_ex_btw,
        _ab_source_file_url,
        _ab_source_file_last_modified
    from source
)

select *
from renamed