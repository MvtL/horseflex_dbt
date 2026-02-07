with webshop as (

select
 --   order_id,
    order_nummer,
    order_datum,
  --  order_timestamp,
    land_code,
    coalesce(nullif(klant_rol, ''), 'Klant') as klant_rol,
   -- totaal_bedrag,
    net_bedrag,
   -- btw_bedrag,
   -- verzend_bedrag
   'Webshop' as source
from {{ ref('stg_orders') }}
),

resellers as (
  
select
 --   order_id,
    order_nummer,
    order_datum,
  --  order_timestamp,
    land_code,
    'Wholesale Customer Special' as klant_rol,
--    totaal_bedrag,
 --   sum(net_bedrag) as net_bedrag,
 net_bedrag,
--    btw_bedrag,
--    verzend_bedrag
    'Reseller' as source

    from {{ ref('stg_orders_resellers') }}
--    Group by 
--     order_nummer,
--      order_datum,
--       land_code  
)

select * from webshop
union all
select * from resellers