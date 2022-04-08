WITH t1 AS (
SELECT 
t.travel_id,
t.description,
t.travel_date,
t.total_distance,
t.avg_speed,
c1.city_name as travel_from,
c2.city_name as travel_to,
u.user_name as driver_name
FROM Travels t LEFT JOIN Cities c1 ON t.travel_from_id = c1.city_id
LEFT JOIN Cities c2 ON t.travel_to_id = c2.city_id
LEFT JOIN Users u ON t.driver_id = u.user_id
),

t2 AS (
SELECT
s.travel_id,
s.stage_order,
s.stage_distance,
s.stage_time,
s.stage_avg_speed,
s.other_passengers,
c1.city_name as stage_city_from,
c2.city_name as stage_city_to
FROM Stages s LEFT JOIN Cities c1 ON s.stage_city_from_id = c1.city_id
LEFT JOIN Cities c2 ON s.stage_city_to_id = c2.city_id
)

SELECT
t1.description,
t1.travel_date,
t2.stage_order,
t2.stage_city_from,
t2.stage_city_to,
t2.stage_distance,
t2.stage_avg_speed,
t2.stage_time,
t1.total_distance,
t1.avg_speed,
t1.driver_name,
t2.other_passengers
FROM t1 LEFT JOIN t2 ON t1.travel_id = t2.travel_id