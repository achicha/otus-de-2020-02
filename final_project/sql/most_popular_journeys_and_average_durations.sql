WITH groupped AS (
  SELECT
    (start_station_id),
    (end_station_id),
    IF(start_station_id = end_station_id,
      TRUE,
      FALSE) same_station,
    ROUND(AVG(duration), 2) AS avg_duration,
    COUNT(*) AS total_rides
  FROM
    `bikes_data.cycle_hire` bh
  GROUP BY
    start_station_id,
    end_station_id,
    same_station
  ORDER BY
    total_rides DESC
)

SELECT bs1.name as start_station_name, bs2.name as end_station_name, same_station, avg_duration, total_rides, bs1.longitude ,bs1.latitude
FROM groupped g
  join `bikes_data.cycle_stations` bs1 on g.start_station_id=bs1.id
  join `bikes_data.cycle_stations` bs2 on g.end_station_id=bs2.id
order by total_rides desc