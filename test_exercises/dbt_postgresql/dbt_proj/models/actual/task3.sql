{{
  config
  (
    tags = ["task"]
  )    
}}

select
  *
  , case
    when row_number() over (partition by client_id order by payment_date) = 1 then 'Новый'
    else 'Действующий'
  end as client_state
from {{ source('public','payments') }}
