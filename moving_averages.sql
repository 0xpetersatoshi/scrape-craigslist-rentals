with q as (
  select
    datetime,
    avg(price) as avg_price,
    avg(price/sqft) as avg_price_per_sqft,
    count(distinct link) as unique_listings
  from "craigslist"."pq_craigslist_rental_data_parquet"
  -- filter out extreme values
  where price <= (select avg(price) * 3 from "craigslist"."pq_craigslist_rental_data_parquet")
  group by 1
  -- filter for days where there are at least more than 10 postings
  having count(distinct link) > 10
)
select 
  q.datetime,
  q.unique_listings,
  q.avg_price,
  avg(q.avg_price) over (order by q.datetime asc rows 6 preceding) as price_7ma,
  avg(q.avg_price) over (order by q.datetime asc rows 29 preceding) as price_30ma,
  q.avg_price_per_sqft,
  avg(q.avg_price_per_sqft) over (order by q.datetime asc rows 6 preceding) as ppsqft_7ma,
  avg(q.avg_price_per_sqft) over (order by q.datetime asc rows 29 preceding) as ppsqft_30ma
from q
order by q.datetime;