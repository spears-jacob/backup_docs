USE ${env:DASP_db};

DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_all_sales PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_daily_sales PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_us_sales PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_us_daily_sales PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_app_details PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_ratings PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_daily_ranks PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_reviews PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_sentiment PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_all_sales (
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
location '${env:warehouse_path}/${env:TMP_db}.db/app_figures_parsed/all_sales';

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_daily_sales (
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
location '${env:warehouse_path}/${env:TMP_db}.db/app_figures_parsed/daily_sales';

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_us_sales (
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
location '${env:warehouse_path}/${env:TMP_db}.db/app_figures_parsed/us_sales';

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_us_daily_sales (
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
location '${env:warehouse_path}/${env:TMP_db}.db/app_figures_parsed/us_daily';

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_app_details (
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
location '${env:warehouse_path}/${env:TMP_db}.db/app_figures_parsed/app_details';

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_ratings (
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
location '${env:warehouse_path}/${env:TMP_db}.db/app_figures_parsed/ratings';

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_daily_ranks (
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
location '${env:warehouse_path}/${env:TMP_db}.db/app_figures_parsed/daily_ranks';

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_reviews (
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
location '${env:warehouse_path}/${env:TMP_db}.db/app_figures_parsed/reviews';

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.app_figures_sentiment (
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
LOCATION '${env:warehouse_path}/${env:TMP_db}.db/app_figures_sentiment';
