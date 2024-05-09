WITH 

stg_title AS (
    SELECT *
    FROM {{ source('VISIONBOOKS', 'TITLES') }}
),
stg_sales AS (
    SELECT *
    FROM {{ source('VISIONBOOKS', 'SALES')}}
),
stg_discounts AS (
    SELECT *
    FROM {{ source('VISIONBOOKS', 'DISCOUNTS')}}
),
stg_readers AS (
    SELECT *
    FROM {{ source('VISIONBOOKS', 'ReaderSales')}}
)
SELECT 
    r.READER_ID,
    -- You can use MIN() or MAX() to ensure unique SALES_ID
    EXTRACT(month FROM s.ord_date) AS month,
    EXTRACT(year FROM s.ord_date) AS year,
    
    SUM(s.QTY) AS total_quantity,
    --SUM(s.QTY * t.PRICE * (d.DISCOUNT / 100)) AS discount_amount,
    SUM(s.QTY * t.PRICE ) AS total_amount
    
FROM 
    stg_title t
JOIN 
    stg_sales s ON s.TITLE_ID = t.TITLE_ID
--JOIN 
    --stg_discounts d ON s.SALES_ID = d.SALES_ID--
JOIN
    stg_readers r ON r.SALES_ID = s.SALES_ID
GROUP BY r.READER_ID,month,year