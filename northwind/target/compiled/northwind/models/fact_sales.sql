with stg_orders as 
(
    select
        orderid,  
        md5(cast(coalesce(cast(employeeid as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as employeekey, 
        md5(cast(coalesce(cast(customerid as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as customerkey,
        md5(cast(coalesce(cast(orderid as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as orderkey,
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey,

    from raw.northwind.Orders
),
stg_order_details as
(
    select 
        orderid,
        md5(cast(coalesce(cast(productid as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as productkey,
        sum(Quantity) as quantity, 
        sum(Quantity*UnitPrice) as extendedpriceamount,
        sum(Quantity*UnitPrice*Discount) as discountamount,
        sum(Quantity*UnitPrice*(1-Discount)) as soldamount
    from raw.northwind.Order_Details
    group by productkey, orderid
)
select
    o.employeekey,
    o.customerkey,
    o.orderkey,
    d.productkey,
    o.orderdatekey,
    d.quantity,
    d.extendedpriceamount,
    d.discountamount,
    d.soldamount
from
    stg_orders o join stg_order_details d on o.orderid = d.orderid