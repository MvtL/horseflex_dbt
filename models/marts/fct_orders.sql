select
    order_id,
    order_nummer,
    order_datum,
    order_timestamp,
    land_code,
    klant_rol,
    totaal_bedrag,
    net_bedrag,
    btw_bedrag,
    verzend_amount

from {{ ref('stg_orders') }}
where order_date is not null