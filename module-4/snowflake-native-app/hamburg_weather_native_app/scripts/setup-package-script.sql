-- ################################################################
-- Create SHARED_CONTENT_SCHEMA to share in the application package
-- ################################################################
CREATE OR REPLACE TABLE tasty_bytes.harmonized.weather_hamburg_t AS
SELECT *
FROM tasty_bytes.harmonized.weather_hamburg;

GRANT reference_usage ON database tasty_bytes TO SHARE IN application package {{ package_name }};

-- now that we can reference our proprietary data, let's create some views
-- this "package schema" will be accessible inside of our setup script
create schema if not exists {{ package_name }}.shared_content_schema;
use schema {{ package_name }}.shared_content_schema;

CREATE OR REPLACE VIEW shared_content_schema.weather_hamburg_app AS SELECT * FROM tasty_bytes.harmonized.weather_hamburg_t;

-- these grants allow our setup script to actually refer to our views
grant usage on schema shared_content_schema
  to share in application package {{ package_name }};
grant select on view shared_content_schema.weather_hamburg_app
  to share in application package {{ package_name }};