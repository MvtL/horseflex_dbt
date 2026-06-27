{{ config(materialized='table') }}

with order_lines as (

    select
        order_line_id,
        order_nummer,
        product_code,
        order_datum,
        bedrijfsnaam,
        land_code,
        verkoopsprijs,
        aantal,
        net_bedrag,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_generation_id
    from {{ ref('stg_order_lines_resellers') }}

)

select * from order_lines
