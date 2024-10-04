USE ${env:ENVIRONMENT};

DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_all_sales;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_daily_sales;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_us_sales;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_us_daily_sales;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_app_details;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_ratings;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_daily_ranks;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_reviews;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_sentiment;

CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.app_figures_downloads (
  product_id string,
  product_name string,
  developer string,
  icon string,
  vendor_identifier string,
  ref_no string,
  sku string,
  package_name string,
  store_id int,
  store string,
  storefront string,
  release_date string,
  added_date string,
  updated_date string,
  version string,
  active boolean,
  source_external_account_id string,
  source_added_timestamp string,
  source_active boolean,
  source_hidden boolean,
  source_type string,
  all_downloads bigint,
  all_re_downloads bigint,
  all_updates bigint,
  all_returns bigint,
  all_net_downloads bigint,
  all_promos bigint,
  all_revenue double,
  all_returns_amount double,
  all_edu_downloads bigint,
  all_gifts bigint,
  all_redemptions bigint,
  daily_downloads bigint,
  daily_re_downloads bigint,
  daily_updates bigint,
  daily_returns bigint,
  daily_net_downloads bigint,
  daily_promos bigint,
  daily_revenue double,
  daily_returns_amount double,
  daily_edu_downloads bigint,
  daily_gifts bigint,
  daily_redemptions bigint,
  us_all_downloads bigint,
  us_all_re_downloads bigint,
  us_all_updates bigint,
  us_all_returns bigint,
  us_all_net_downloads bigint,
  us_all_promos bigint,
  us_all_revenue double,
  us_all_returns_amount double,
  us_all_edu_downloads bigint,
  us_all_gifts bigint,
  us_all_redemptions bigint,
  us_daily_downloads bigint,
  us_daily_re_downloads bigint,
  us_daily_updates bigint,
  us_daily_returns bigint,
  us_daily_net_downloads bigint,
  us_daily_promos bigint,
  us_daily_revenue double,
  us_daily_returns_amount double,
  us_daily_edu_downloads bigint,
  us_daily_gifts bigint,
  us_daily_redemptions bigint,
  rating_region string,
  one_star bigint,
  two_star bigint,
  thr_star bigint,
  fou_star bigint,
  fiv_star bigint
)
partitioned by (partition_date_denver string)
stored as orc TBLPROPERTIES("orc.compress"="SNAPPY");

CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.app_figures_ranks (
  product_id string,
  country string,
  category_store string,
  category_store_id int,
  category_device string,
  category_device_id int,
  category_id string,
  category_name string,
  category_subtype string,
  position bigint,
  delta int
)
partitioned by (partition_date_denver string)
stored as orc TBLPROPERTIES("orc.compress"="SNAPPY");

CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.app_figures_sentiment (
  author string,
  title string,
  review string,
  original_title string,
  original_review string,
  stars double,
  iso string,
  version string,
  product_name string,
  product_id string,
  vendor_id string,
  store string,
  weight int,
  id string,
  language string,
  sentiment double
)
partitioned by (partition_date_denver string)
stored as orc TBLPROPERTIES("orc.compress"="SNAPPY");

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_all_sales (
  downloads bigint,
  re_downloads bigint,
  updates bigint,
  returns bigint,
  net_downloads bigint,
  promos bigint,
  revenue double,
  returns_amount double,
  edu_downloads bigint,
  gifts bigint,
  gift_redemptions bigint,
  product_id string,
  partition_date_denver string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    'separatorChar' = ',',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
location '/apps/hive/warehouse/${env:TMP_db}.db/app_figures_parsed/all_sales';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_daily_sales (
  downloads bigint,
  re_downloads bigint,
  updates bigint,
  returns bigint,
  net_downloads bigint,
  promos bigint,
  revenue double,
  returns_amount double,
  edu_downloads bigint,
  gifts bigint,
  gift_redemptions bigint,
  product_id string,
  partition_date_denver string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    'separatorChar' = ',',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
location '/apps/hive/warehouse/${env:TMP_db}.db/app_figures_parsed/daily_sales';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_us_sales (
  downloads bigint,
  re_downloads bigint,
  updates bigint,
  returns bigint,
  net_downloads bigint,
  promos bigint,
  revenue double,
  returns_amount double,
  edu_downloads bigint,
  gifts bigint,
  gift_redemptions bigint,
  product_id string,
  partition_date_denver string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    'separatorChar' = ',',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
location '/apps/hive/warehouse/${env:TMP_db}.db/app_figures_parsed/us_sales';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_us_daily_sales (
  downloads bigint,
  re_downloads bigint,
  updates bigint,
  returns bigint,
  net_downloads bigint,
  promos bigint,
  revenue double,
  returns_amount double,
  edu_downloads bigint,
  gifts bigint,
  gift_redemptions bigint,
  product_id string,
  partition_date_denver string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    'separatorChar' = ',',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
location '/apps/hive/warehouse/${env:TMP_db}.db/app_figures_parsed/us_daily';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_app_details (
  product_id string,
  product_name string,
  developer string,
  icon string,
  vendor_identifier string,
  ref_no string,
  sku string,
  package_name string,
  store_id int,
  store string,
  storefront string,
  release_date string,
  added_date string,
  updated_date string,
  version string,
  active boolean,
  source_external_account_id string,
  source_added_timestamp string,
  source_active boolean,
  source_hidden boolean,
  source_type string,
  partition_date_denver string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    'separatorChar' = ',',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
location '/apps/hive/warehouse/${env:TMP_db}.db/app_figures_parsed/app_details';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_ratings (
  product_id string,
  region string,
  one_star bigint,
  two_star bigint,
  thr_star bigint,
  fou_star bigint,
  fiv_star bigint,
  partition_date_denver string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    'separatorChar' = ',',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
location '/apps/hive/warehouse/${env:TMP_db}.db/app_figures_parsed/ratings';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_daily_ranks (
  product_id string,
  country string,
  category_store string,
  category_store_id int,
  category_device string,
  category_device_id int,
  category_id string,
  category_name string,
  category_subtype string,
  position bigint,
  delta int,
  partition_date_denver string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    'separatorChar' = ',',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
location '/apps/hive/warehouse/${env:TMP_db}.db/app_figures_parsed/daily_ranks';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_reviews (
  author string,
  title string,
  review string,
  original_title string,
  original_review string,
  stars double,
  iso string,
  version string,
  partition_date_denver string,
  product_id string,
  product_name string,
  vendor_id string,
  store string,
  weight int,
  id string,
  language string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    'separatorChar' = ',',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
location '/apps/hive/warehouse/${env:TMP_db}.db/app_figures_parsed/reviews';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_sentiment (
  author string,
  title string,
  review string,
  original_title string,
  original_review string,
  stars double,
  iso string,
  version string,
  partition_date_denver string,
  product_id string,
  product_name string,
  vendor_id string,
  store string,
  weight int,
  id string,
  language string,
  sentiment double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES
(
    'separatorChar' = ',',
    'quoteChar'     = '\"'
)
STORED AS TEXTFILE
LOCATION '/apps/hive/warehouse/${env:TMP_db}.db/app_figures_sentiment';

-- CREATE TABLE IF NOT EXISTS prod_tmp.app_figures_sentiment (
--   author string,
--   title string,
--   review string,
--   original_title string,
--   original_review string,
--   stars double,
--   iso string,
--   version string,
--   product_name string,
--   product_id string,
--   vendor_id string,
--   store string,
--   weight int,
--   id string,
--   language string,
--   partition_date_denver string,
--   sentiment double
-- )
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
-- WITH SERDEPROPERTIES
-- (
--     'separatorChar' = ',',
--     'quoteChar'     = '\"'
-- )
-- STORED AS TEXTFILE
-- LOCATION '/apps/hive/warehouse/prod_tmp.db/app_figures_sentiment';
