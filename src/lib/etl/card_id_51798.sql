select
    distinct tax_code
        , trim(lower(company_name)) as company_name
        , update_time
        , user_id
from custom.invoice_user
where true
    and is_delete = false
    and length(tax_code) > 0
    and date(update_time,'Asia/Saigon') >= date({update_time},'Asia/Saigon')
order by 3