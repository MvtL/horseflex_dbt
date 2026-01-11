select
    order_id,
    order_number,
    order_date,
    order_timestamp,
    country_code,
    customer_role,

    total_amount,
    net_amount,
    tax_amount,
    shipping_amount

from {{ ref('stg_orders') }}
where order_date is not null