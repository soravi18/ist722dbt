
  
    

        create or replace transient table analytics.dbt_sravi_northwind.dim_product
         as
        (select 
    md5(cast(coalesce(cast(p.productid as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) 
       as productkey, 
    p.productid,
    p.productname,
    p.supplierid,
    c.categoryname,
    c.description as categorydescription
from raw.northwind.Products p
    inner join raw.northwind.Categories c on p.categoryid = c.categoryid
        );
      
  