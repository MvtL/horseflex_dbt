with source as (

    select *
    from {{ source('airbyte_landing', 'Webshop_export_orders') }}

),

renamed as (

    select
        _airbyte_raw_id                       as order_id,
        cast(Order_Number as string)         as order_nummer,
    --    Order_Date                           as order_timestamp,
        date(Order_Date)                     as order_datum,

        Country_Code__Shipping_              as land_code,
        Customer_Role                        as klant_rol,

        Order_Total_Amount                   as totaal_bedrag,
        Order_Total_Amount_without_Tax       as net_bedrag,
        Order_Total_Tax_Amount               as btw_bedrag,
        Order_Shipping_Amount                as verzend_bedrag,

        _airbyte_extracted_at                as extracted_at

    from source
)

select *
from renamed