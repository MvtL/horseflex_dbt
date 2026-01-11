with weekly as (
  select
    date_trunc(order_date, week(monday)) as week_start,
    country_code,
    count(*) as orders,
    sum(total_amount) as revenue
  from {{ ref('fct_orders') }}
  group by 1,2
),

wow as (
  select
    *,
    lag(revenue) over (partition by country_code order by week_start) as prev_week_revenue,
    revenue - lag(revenue) over (partition by country_code order by week_start) as revenue_change,
    safe_divide(
      revenue - lag(revenue) over (partition by country_code order by week_start),
      lag(revenue) over (partition by country_code order by week_start)
    ) as revenue_growth_pct
  from weekly
)

select *
from wow