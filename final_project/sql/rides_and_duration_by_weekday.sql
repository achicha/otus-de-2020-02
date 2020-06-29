SELECT
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM start_date) = 1 THEN '2. Sun'
    WHEN EXTRACT(DAYOFWEEK
  FROM
    start_date) = 2 THEN '3. Mon'
    WHEN EXTRACT(DAYOFWEEK FROM start_date) = 3 THEN '4. Tue'
    WHEN EXTRACT(DAYOFWEEK
  FROM
    start_date) = 4 THEN '5. Wed'
    WHEN EXTRACT(DAYOFWEEK FROM start_date) = 5 THEN '6. Thu'
    WHEN EXTRACT(DAYOFWEEK
  FROM
    start_date) = 6 THEN '7. Fri'
    WHEN EXTRACT(DAYOFWEEK FROM start_date) = 7 THEN '1. Sat'
END
  AS day_of_week,
  COUNT(*) number_of_rides,
  ROUND(AVG(duration),2) AS avg_duration
FROM
  `bikes_data.cycle_hire`
GROUP BY
  day_of_week
ORDER BY
  day_of_week ASC