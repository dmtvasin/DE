select 
*,
Case when row_number() over (partition by client_id order by payment_date) = 1 Then 'Новый'
    Else 'Действующий'
End client_state
from {{ source('public','payments') }} 
