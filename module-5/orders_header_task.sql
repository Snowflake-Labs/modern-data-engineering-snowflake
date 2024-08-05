USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;

-- Task that runs executes every minute
CREATE OR REPLACE TASK tasty_bytes.raw_pos.process_orders_header_sproc
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON * * * * * UTC'
AS
CALL tasty_bytes.raw_pos.process_order_headers_stream();

-- Activate the task to run
ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc RESUME;

-- Query the table
SELECT * FROM tasty_bytes.raw_pos.daily_sales_hamburg_t;

-- Insert some dummy data into ORDER_HEADER
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
    4494,                          -- LOCATION_ID
    null,                          -- CUSTOMER_ID
    null,                          -- DISCOUNT_ID
    123456789,                     -- SHIFT_ID
    '08:00:00',                    -- SHIFT_START_TIME
    '16:00:00',                    -- SHIFT_END_TIME
    null,                          -- ORDER_CHANNEL
    '2024-01-12 12:30:45',         -- ORDER_TS
    null,                          -- SERVED_TS
    'USD',                         -- ORDER_CURRENCY
    22.00,                         -- ORDER_AMOUNT
    null,                          -- ORDER_TAX_AMOUNT
    null,                          -- ORDER_DISCOUNT_AMOUNT
    24.50                          -- ORDER_TOTAL
);

-- Wait 1 minute before running this, query the table once more
SELECT * FROM tasty_bytes.raw_pos.daily_sales_hamburg_t;

-- Suspend the task
ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc SUSPEND;


-- Optional: recreate the task such that it executes every 24 hours
-- CREATE OR REPLACE TASK tasty_bytes.raw_pos.process_orders_header_sproc
-- SCHEDULE = 'USING CRON 0 0 * * * UTC'
-- AS
-- CALL tasty_bytes.raw_pos.process_order_headers_stream();

-- Optional: Start the task
-- ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc RESUME;

-- Required: Stop the task if you started it using the command directly above this one
-- ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc SUSPEND;