select 
client_name, 
payment_date,
max(value)
from {{ source('public','payments') }} 
where payment_date between '2023-01-01' and '2023-01-31'
Group by 1, 2
