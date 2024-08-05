USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;

CREATE OR REPLACE DYNAMIC TABLE tasty_bytes.raw_pos.daily_sales_hamburg
WAREHOUSE = 'COMPUTE_WH'
TARGET_LAG =  
AS
SELECT
    CAST(oh.ORDER_TS AS DATE) AS date,
    COALESCE(SUM(oh.ORDER_TOTAL), 0) AS total_sales
FROM
    tasty_bytes.raw_pos.order_header oh
JOIN
    tasty_bytes.raw_pos.location loc
ON
    oh.LOCATION_ID = loc.LOCATION_ID
WHERE
    loc.CITY = 'Hamburg'
    AND loc.COUNTRY = 'Germany'
GROUP BY
    CAST(oh.ORDER_TS AS DATE);

SELECT * FROM tasty_bytes.raw_pos.daily_sales_hamburg;

-- Insert dummy data for a sale occurring at a location in Hamburg
INSERT INTO tasty_bytes.raw_pos.order_header (
    ORDER_ID, 
    TRUCK_ID, 
    LOCATION_ID, 
    CUSTOMER_ID, 
    DISCOUNT_ID, 
    SHIFT_ID, 
    SHIFT_START_TIME, 
    SHIFT_END_TIME, 
    ORDER_CHANNEL, 
    ORDER_TS, 
    SERVED_TS, 
    ORDER_CURRENCY, 
    ORDER_AMOUNT, 
    ORDER_TAX_AMOUNT, 
    ORDER_DISCOUNT_AMOUNT, 
    ORDER_TOTAL
) VALUES (
    123456789,                     -- ORDER_ID
    101,                           -- TRUCK_ID
    4493,                          -- LOCATION_ID
    null,                          -- CUSTOMER_ID
    null,                          -- DISCOUNT_ID
    123456789,                     -- SHIFT_ID
    '08:00:00',                    -- SHIFT_START_TIME
    '16:00:00',                    -- SHIFT_END_TIME
    null,                          -- ORDER_CHANNEL
    '2024-03-09 12:30:45',         -- ORDER_TS
    null,                          -- SERVED_TS
    'USD',                         -- ORDER_CURRENCY
    12.00,                         -- ORDER_AMOUNT
    null,                          -- ORDER_TAX_AMOUNT
    null,                          -- ORDER_DISCOUNT_AMOUNT
    12.35                          -- ORDER_TOTAL
);

SELECT * FROM tasty_bytes.raw_pos.daily_sales_hamburg;
