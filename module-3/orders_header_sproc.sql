USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;

-- Create the stored procedure, define its logic with Snowpark for Python, write sales to raw_pos.daily_sales_hamburg_t
CREATE OR REPLACE PROCEDURE tasty_bytes.raw_pos.process_order_headers_stream()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  HANDLER ='process_order_headers_stream'
  PACKAGES = ('snowflake-snowpark-python')
AS
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session

def process_order_headers_stream(session: Session) -> float:
    # Query the stream
    recent_orders = session.table("order_header_stream").filter(F.col("METADATA$ACTION") == "INSERT")
    
    # Look up location of the orders in the stream using the LOCATIONS table
    locations = session.table("location")
    hamburg_orders = recent_orders.join(
        locations,
        recent_orders["LOCATION_ID"] == locations["LOCATION_ID"]
    ).filter(
        (locations["CITY"] == "Hamburg") &
        (locations["COUNTRY"] == "Germany")
    )
    
    # Calculate the sum of sales in Hamburg
    total_sales = hamburg_orders.group_by(F.date_trunc('DAY', F.col("ORDER_TS"))).agg(
        F.coalesce(F.sum("ORDER_TOTAL"), F.lit(0)).alias("total_sales")
    )
    
    # Select the columns with proper aliases and convert to date type
    daily_sales = total_sales.select(
        F.col("DATE_TRUNC('DAY', ORDER_TS)").cast("DATE").alias("DATE"),
        F.col("total_sales")
    )
    
    # Write the results to the DAILY_SALES_HAMBURG_T table
    total_sales.write.mode("append").save_as_table("raw_pos.daily_sales_hamburg_t")
    
    # Return a message indicating the operation was successful
    return "Daily sales for Hamburg, Germany have been successfully written to raw_pos.daily_sales_hamburg_t"
$$;


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
    '2023-07-01 12:30:45',         -- ORDER_TS
    null,                          -- SERVED_TS
    'USD',                         -- ORDER_CURRENCY
    41.30,                         -- ORDER_AMOUNT
    null,                          -- ORDER_TAX_AMOUNT
    null,                          -- ORDER_DISCOUNT_AMOUNT
    45.80                          -- ORDER_TOTAL
);

-- Confirm the insert
SELECT * FROM tasty_bytes.raw_pos.order_header WHERE location_id = 4493;

-- Call the stored procedure
CALL tasty_bytes.raw_pos.process_order_headers_stream();

-- Confirm the insert to the daily_sales_hamburg_t table
SELECT * FROM tasty_bytes.raw_pos.daily_sales_hamburg_t;