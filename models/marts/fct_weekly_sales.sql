with weekly as (
  select
    date_trunc(order_date, week(monday)) as week_start,
    land_code,
    count(*) as orders,
    sum(totaal_bedrag) as revenue
  from {{ ref('fct_orders') }}
  group by 1,2
),

wow as (
  select
    *,
    lag(revenue) over (partition by country_code order by week_start) as vorige_week_omzet,
    revenue - lag(revenue) over (partition by country_code order by week_start) as omzet_verschil,
  from weekly
)

select *
from wow