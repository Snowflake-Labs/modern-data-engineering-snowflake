USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;

CREATE OR REPLACE STREAM order_header_stream ON TABLE tasty_bytes.raw_pos.order_header;

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
    1234,                          -- LOCATION_ID
    null,                          -- CUSTOMER_ID
    null,                          -- DISCOUNT_ID
    123456789,                     -- SHIFT_ID
    '08:00:00',                    -- SHIFT_START_TIME
    '16:00:00',                    -- SHIFT_END_TIME
    null,                          -- ORDER_CHANNEL
    '2023-07-01 12:30:45',         -- ORDER_TS
    null,                          -- SERVED_TS
    'USD',                         -- ORDER_CURRENCY
    50.00,                         -- ORDER_AMOUNT
    null,                          -- ORDER_TAX_AMOUNT
    null,                          -- ORDER_DISCOUNT_AMOUNT
    52.50                          -- ORDER_TOTAL
);

SELECT * FROM order_header_stream;

DELETE FROM tasty_bytes.raw_pos.order_header WHERE order_id=123456789;

-- This won't return the deleted action in the stream because of how standard streams work
-- See: https://docs.snowflake.com/en/user-guide/streams-intro#types-of-streams
SELECT * FROM order_header_stream;

