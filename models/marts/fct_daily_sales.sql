select
  order_date,
  country_code,
  count(*) as orders,
  sum(total_amount) as revenue,
  sum(net_amount) as net_revenue
from {{ ref('fct_orders') }}
group by 1,2