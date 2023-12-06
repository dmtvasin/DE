with cte_payments as (
    select
    {{ dbt.date_trunc('month', 'payment_date') }} as payment_date,
    sum(value) as revenue_by_month
    from {{ source('public','payments') }}
    Group by 1
)
select
TO_CHAR(payment_date, 'FMMonth YYYY') as period,
revenue_by_month,
sum(revenue_by_month) OVER (ORDER BY payment_date) AS cumulative_sum
from cte_payments