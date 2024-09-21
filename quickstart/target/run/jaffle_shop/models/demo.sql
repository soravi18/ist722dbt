
  
    

        create or replace transient table raw.dbt_sravi.demo
         as
        (select * from raw.jaffle_shop.customers
        );
      
  