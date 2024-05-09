<<<<<<< HEAD
WITH stg_titleauthors AS (
          SELECT * FROM {{ source('VISIONBOOKS', 'TITLEAUTHORS') }}
      ),
stg_authors as 
(
    select *
        
        
    from {{ source('VISIONBOOKS','AUTHORS') }}
),
stg_title as
(
    select *
       
    from {{ source('VISIONBOOKS', 'TITLES') }}
),
stg as (
select ta.AU_ID as authorkey,
sum(t.ROYALTY) as royalty_sum
from 
    stg_title t
join stg_titleauthors ta on ta.TITLE_ID=t.TITLE_ID
join stg_authors a on ta.AU_ID=a.AU_ID
group by authorkey
)

select royalty_sum,authorkey
=======
WITH stg_titleauthors AS (
          SELECT * FROM {{ source('VISIONBOOKS', 'TITLEAUTHORS') }}
      ),
stg_authors as 
(
    select *
        
        
    from {{ source('VISIONBOOKS','AUTHORS') }}
),
stg_title as
(
    select *
       
    from {{ source('VISIONBOOKS', 'TITLES') }}
),
stg as (
select ta.AU_ID as authorkey,
sum(t.ROYALTY) as royalty_sum
from 
    stg_title t
join stg_titleauthors ta on ta.TITLE_ID=t.TITLE_ID
join stg_authors a on ta.AU_ID=a.AU_ID
group by authorkey
)

select royalty_sum,authorkey
>>>>>>> 29c3079b6f09754d10b29231edd35720f6ddd567
from stg s