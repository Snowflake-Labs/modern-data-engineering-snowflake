USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

-- Define the target table
CREATE OR REPLACE TABLE load_data.public.sample_menu_copy_into
(
   menu_id NUMBER(19,0),
   menu_type_id NUMBER(38,0),
   menu_type VARCHAR(16777216),
   truck_brand_name VARCHAR(16777216),
   menu_item_id NUMBER(38,0),
   menu_item_name VARCHAR(16777216),
   item_category VARCHAR(16777216),
   item_subcategory VARCHAR(16777216),
   cost_of_goods_usd NUMBER(38,4),
   sale_price_usd NUMBER(38,4),
   menu_item_health_metrics_obj VARIANT
);

-- Create external stage pointing to an AWS S3 bucket
-- Specify the file format
CREATE OR REPLACE STAGE load_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);

-- Query the stage to note the file we'll load
LIST @load_data.public.blob_stage/raw_pos/menu/;

--- Copy the file into the target table
COPY INTO load_data.public.sample_menu_copy_into
FROM @load_data.public.blob_stage/raw_pos/menu/;

-- Sanity check: query the newly loaded data
SELECT * FROM load_data.public.sample_menu_copy_into;
