USE ROLE accountadmin;

CREATE WAREHOUSE IF NOT EXISTS compute_wh;
USE WAREHOUSE compute_wh;

-- Create a database and schema to store our data
CREATE OR REPLACE DATABASE wages_cpi;
CREATE OR REPLACE SCHEMA data;
USE DATABASE wages_cpi;
USE SCHEMA data;

-- Creates a table tracking average annual wages and CPI for the USA, between 2012 and 2022
CREATE OR REPLACE TABLE annual_wages_cpi_usa AS
SELECT
  DATE_TRUNC('year', oecd_timeseries.date) AS year,
  ROUND(
    AVG(
      CASE 
        WHEN oecd_attributes.variable_name ILIKE '%annual wages%' THEN oecd_timeseries.value
        ELSE NULL 
      END
    )
  ) AS avg_annual_wages,
  ROUND(
    AVG(
      CASE 
        WHEN bureau_of_labor_statistics_price_attributes.variable_name ILIKE '%CPI%' THEN bureau_of_labor_statistics_price_timeseries.value
        ELSE NULL 
      END
    )
  ) AS cpi
FROM
  Finance__Economics.CYBERSYN.OECD_TIMESERIES oecd_timeseries
JOIN 
  Finance__Economics.CYBERSYN.OECD_ATTRIBUTES oecd_attributes
  ON oecd_timeseries.variable = oecd_attributes.variable
LEFT JOIN 
  Finance__Economics.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES bureau_of_labor_statistics_price_timeseries
  ON DATE_TRUNC('year', oecd_timeseries.date) = DATE_TRUNC('year', bureau_of_labor_statistics_price_timeseries.date)
  AND bureau_of_labor_statistics_price_timeseries.geo_id = 'country/USA'
LEFT JOIN 
  Finance__Economics.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES bureau_of_labor_statistics_price_attributes
  ON bureau_of_labor_statistics_price_timeseries.variable = bureau_of_labor_statistics_price_attributes.variable
WHERE
  (oecd_attributes.variable_name ILIKE '%annual wages%' 
  OR bureau_of_labor_statistics_price_attributes.variable_name ILIKE '%CPI%')
  AND oecd_timeseries.geo_id = 'country/USA'
  AND DATE_TRUNC('year', oecd_timeseries.date) BETWEEN '2012-01-01' AND '2022-12-31'
GROUP BY
  year;


-- Creates a table tracking CPI for the USA over the last 3 years, on a monthly basis
CREATE OR REPLACE TABLE monthly_cpi_usa AS
SELECT
  DATE_TRUNC('month', bureau_of_labor_statistics_price_timeseries.date) AS month,
  ROUND(
    AVG(
      CASE 
        WHEN bureau_of_labor_statistics_price_attributes.variable_name ILIKE '%CPI%' THEN bureau_of_labor_statistics_price_timeseries.value
        ELSE NULL 
      END
    ), 1
  ) AS avg_cpi
FROM
  Finance__Economics.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_TIMESERIES bureau_of_labor_statistics_price_timeseries
JOIN 
  Finance__Economics.CYBERSYN.BUREAU_OF_LABOR_STATISTICS_PRICE_ATTRIBUTES bureau_of_labor_statistics_price_attributes 
  ON bureau_of_labor_statistics_price_timeseries.variable = bureau_of_labor_statistics_price_attributes.variable
WHERE
  bureau_of_labor_statistics_price_attributes.variable_name ILIKE '%CPI%'
  AND bureau_of_labor_statistics_price_timeseries.geo_id = 'country/USA'
  AND DATE_TRUNC('month', bureau_of_labor_statistics_price_timeseries.date) BETWEEN DATEADD(year, -3, CURRENT_DATE) AND CURRENT_DATE
GROUP BY
  month;

SELECT * FROM annual_wages_cpi_usa;
SELECT * FROM monthly_cpi_usa;
