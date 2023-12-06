{{
  config
  (
    tags = ["task"]
  )    
}}

with cte_payments as (
  select
    {{ dbt.date_trunc('month', 'payment_date') }} as payment_date
    , sum(value) as revenue_by_month
  from {{ source('public','payments') }}
  group by 1
)

select
  to_char(payment_date, 'FMMonth YYYY') as period
  , revenue_by_month
  , sum(revenue_by_month) over (order by payment_date) as cumulative_sum
from cte_payments
