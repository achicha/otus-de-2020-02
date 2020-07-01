SELECT
  EXTRACT(WEEK FROM start_date) week_number
  , COUNT(*) number_of_rides
  , ROUND(AVG(duration),2) AS avg_duration
FROM `platinum-trees-277807.bikes_data.cycle_hire`
group by 1
order by 1