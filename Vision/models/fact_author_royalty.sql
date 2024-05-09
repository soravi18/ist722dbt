with
    stg_titleauthors as (select * from {{ source("VISIONBOOKS", "TITLEAUTHORS") }}),
    stg_authors as (select * from {{ source("VISIONBOOKS", "AUTHORS") }}),
    stg_title as (select * from {{ source("VISIONBOOKS", "TITLES") }})
        select ta.au_id as authorkey, sum(t.royalty) as royalty_sum
        from stg_title t
        join stg_titleauthors ta on ta.title_id = t.title_id
        join stg_authors a on ta.au_id = a.au_id
        group by authorkey
    
