-- Use the `ref` function to select from other models

select *
from raw.dbt_sravi.my_first_dbt_model
where id = 1