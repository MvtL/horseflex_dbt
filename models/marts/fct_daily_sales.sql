select
  order_date,
  country_code,
  count(*) as orders,
  sum(totaal_bedrag) as omzet_perdag,
  sum(net_bedrag) as omzet_net_perdag
from {{ ref('fct_orders') }}
group by 1,2