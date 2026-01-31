select
    order_id,
    order_nummer,
    order_datum,
  --  order_timestamp,
    land_code,
    coalesce(nullif(klant_rol, ''), 'Klant') as klant_rol,
    totaal_bedrag,
    net_bedrag,
    btw_bedrag,
    verzend_bedrag

from {{ ref('stg_orders') }}