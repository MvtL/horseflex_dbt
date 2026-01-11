with source as (

    select *
    from {{ source('airbyte_landing', 'Webshop_export_orders') }}

),

renamed as (

    select
        _airbyte_raw_id                       as order_id,
        cast(Order_Number as string)         as order_number,
        Order_Date                           as order_timestamp,
        date(Order_Date)                     as order_date,

        Country_Code__Shipping_              as country_code,
        Customer_Role                        as customer_role,

        Order_Total_Amount                   as total_amount,
        Order_Total_Amount_without_Tax       as net_amount,
        Order_Total_Tax_Amount               as tax_amount,
        Order_Shipping_Amount                as shipping_amount,

        _airbyte_extracted_at                as extracted_at

    from source
)

select *
from renamed