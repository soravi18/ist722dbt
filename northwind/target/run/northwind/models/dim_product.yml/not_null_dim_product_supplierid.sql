select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select supplierid
from analytics.dbt_sravi_northwind.dim_product
where supplierid is null



      
    ) dbt_internal_test