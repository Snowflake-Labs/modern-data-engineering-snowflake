-- Returns the average temperature and total precipitation for cities in France
SELECT
  city_name,
  country,
  AVG(avg_temperature_air_2m_f) AS avg_temperature,
  SUM(tot_precipitation_in) AS total_precipitation
FROM
  history_day
WHERE
  date_valid_std >= DATEADD (DAY, -7, CURRENT_DATE) 
  AND country = 'FR'
GROUP BY
  city_name,
  country
ORDER BY
  total_precipitation DESC;