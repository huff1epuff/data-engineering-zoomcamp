Homework 1 SQL commands

Q-3
select 
	count(1)
from 
	green_taxi_trips
where
	date_trunc('day',lpep_pickup_datetime) between '2025-11-01' and '2025-11-30'
	and trip_distance <= 1;

Q-4
select 
	cast(lpep_pickup_datetime as DATE) as "day",
	max(trip_distance) as "max_distance"
from 
	green_taxi_trips
where
	trip_distance < 100
group by 
	"day"
order by 
	"max_distance" desc;

Q-5
SELECT
    sum(total_amount) as "total",
    zpu."Zone" AS "pickup_loc"
FROM
    green_taxi_trips t,
    zones zpu
WHERE
    t."PULocationID" = zpu."LocationID"
	and date_trunc('day', lpep_pickup_datetime) = '2025-11-18'
group by
	2
order by 
	"total" desc
LIMIT 100;

Q-6
SELECT
    zpu."Zone" AS "pickup_loc",
	zdo."Zone" AS "dropoff_loc",
	max(tip_amount) as "largest_tip"
FROM
    green_taxi_trips t,
    zones zpu,
	zones zdo
WHERE
    t."PULocationID" = zpu."LocationID"
	and t."DOLocationID" = zdo."LocationID"
	and date_trunc('day', lpep_pickup_datetime) between '2025-11-01' and '2025-11-30'
	and zpu."Zone" = 'East Harlem North'
group by
	1,2
order by 
	"largest_tip" desc
LIMIT 100;

