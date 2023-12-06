with cte_departments as (
    Select
    overlay(email PLACING '' FROM position('.' IN email) FOR 1) as email  
    ,Case When department is null Then 'Отдел не определен'
        Else department
    End department
    from {{ source('public','manager_departments') }} 
)
Select
cte_departments.department,
sum(payments.value)
From {{ source('public','payments') }} as payments
Left join cte_departments as cte_departments on cte_departments.email = payments.manager_email
Group by 1