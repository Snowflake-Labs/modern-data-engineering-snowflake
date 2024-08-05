USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;

CREATE OR REPLACE FUNCTION tasty_bytes.analytics.fahrenheit_to_celsius(temp_f NUMBER(35,4))
  RETURNS NUMBER(35,4)
  AS
  $$
    (temp_f - 32) * (5/9)
  $$
;

CREATE OR REPLACE FUNCTION tasty_bytes.analytics.inch_to_millimeter(inch NUMBER(35,4))
  RETURNS NUMBER(35,4)
  AS
  $$
    inch * 25.4
  $$
;