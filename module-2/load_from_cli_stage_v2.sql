CREATE OR REPLACE TABLE load_data.public.sample_orders_cli_v2 ( 
    ORDER_ID NUMBER(38, 0), 
    TRUCK_ID NUMBER(38, 0), 
    LOCATION_ID NUMBER(38, 0), 
    CUSTOMER_ID NUMBER(38, 0), 
    DISCOUNT_ID VARCHAR, 
    SHIFT_ID NUMBER(38, 0), 
    SHIFT_START_TIME TIME, 
    SHIFT_END_TIME TIME, 
    ORDER_CHANNEL VARCHAR, 
    ORDER_TS TIMESTAMP_NTZ, 
    SERVED_TS VARCHAR, 
    ORDER_CURRENCY VARCHAR, 
    ORDER_AMOUNT NUMBER(38, 4), 
    ORDER_TAX_AMOUNT VARCHAR, 
    ORDER_DISCOUNT_AMOUNT VARCHAR, 
    ORDER_TOTAL NUMBER(38, 4) 
); 



COPY INTO load_data.public.sample_orders_cli_v2
FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
	FROM @stagetest)
FILES = ('orders1.csv', 'orders2.csv') 
FILE_FORMAT = ( FORMAT_NAME='load_data.public.file_format_cli')
ON_ERROR=ABORT_STATEMENT 