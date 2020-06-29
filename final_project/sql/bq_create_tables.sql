CREATE TABLE `bikes_data.cycle_hire`
PARTITION BY date(start_date)
CLUSTER BY start_station_id, end_station_id, rental_id, duration

AS

SELECT * FROM `bigquery-public-data.london_bicycles.cycle_hire`

--##########################################

CREATE TABLE `bikes_data.cycle_stations`
PARTITION BY install_date
CLUSTER BY id, bikes_count

AS

SELECT * FROM `bigquery-public-data.london_bicycles.cycle_stations`