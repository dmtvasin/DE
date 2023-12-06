{{
  config
  (
    tags = ["task"]
  )    
}}

with cte_departments as (
  select
    overlay(email placing '' from position('.' in email) for 1) as email
    , case
      when department is null then 'Отдел не определен'
      else department
    end as department
  from {{ source('public','manager_departments') }}
)

select
  cte_departments.department
  , sum(payments.value)
from {{ source('public','payments') }} as payments
left join cte_departments as cte_departments on cte_departments.email = payments.manager_email
group by 1
