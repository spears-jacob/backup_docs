use ${env:ENVIRONMENT};


SELECT "Creating specguide_wb_netflix_events table if it does not exist";

DROP TABLE dev.specguide_wb_netflix_events;
CREATE TABLE IF NOT EXISTS dev.specguide_wb_netflix_events(
  visit__visit_id string,
  visit__device__uuid string,
  account__mac_id_aes256_standard string,
  account__number_aes256 string,
  message__category string,
  message__name string,
  message__sequence_number int,
  received__timestamp bigint,
  visit__application_details__app_version string,
  state__view__current_page__page_type string,
  state__view__current_page__elements__standardized_name string,
  state__view__current_page__elements__ui_name string,
  state__view__modal__name string,
  state__view__content__identifiers__tms_guide_id string,
  state__view__content__identifiers__tms_program_id string,
  source_application_name string,
  upgrade_visit_id string,
  launch_visit_id string,
  failed_launch_visit_id string,
  source_criteria string,
  visit__application_details__environment string,
  load_date string)
PARTITIONED BY (partition_date_denver string)
STORED AS ORC
TBLPROPERTIES (
 'ORC.COMPRESS'='SNAPPY',
 'ORC.COMPRESS.SIZE'='8192')
;

SELECT "Creating temp specguide_wb_netflix_events table if it does not exist";


-- CREATE TABLE IF NOT EXISTS ${env:TMP_db}.specguide_wb_netflix_events
DROP TABLE dev_tmp.specguide_wb_netflix_events;
CREATE TABLE IF NOT EXISTS dev_tmp.specguide_wb_netflix_events
  (
  visit__visit_id string,
  visit__device__uuid string,
  account__mac_id_aes256_standard string,
  account__number_aes256 string,
  message__category string,
  message__name string,
  message__sequence_number int,
  received__timestamp bigint,
  visit__application_details__app_version string,
  state__view__current_page__page_type string,
  state__view__current_page__elements__standardized_name string,
  state__view__current_page__elements__ui_name string,
  state__view__modal__name string,
  state__view__content__identifiers__tms_guide_id string,
  state__view__content__identifiers__tms_program_id string,
  source_application_name string,
  upgrade_visit_id string,
  launch_visit_id string,
  failed_launch_visit_id string,
  source_criteria string,
  visit__application_details__environment string,
  load_date string,
  partition_date_denver string
  )
STORED AS ORC
TBLPROPERTIES (
 'ORC.COMPRESS'='SNAPPY',
 'ORC.COMPRESS.SIZE'='8192')
;


SELECT "Creating specguide_worldbox_activity table if it does not exist";

-- Create table to hold VOD and Netflix data for WorldBoxes
-- CREATE TABLE IF NOT EXISTS specguide_worldbox_activity
DROP TABLE dev.specguide_worldbox_activity;
CREATE TABLE IF NOT EXISTS dev.specguide_worldbox_activity
  (
  account__mac_id_aes256_standard string,
  account__number_aes256 string,
  account__type string,
  customer__type string,
  application string,
  application_subtype string,
  source_application_name string,
  message__name string,
  data_source string,
  load_date string
  )
PARTITIONED BY (
  partition_date_denver string)
STORED AS ORC
TBLPROPERTIES (
 'ORC.COMPRESS'='SNAPPY',
 'ORC.COMPRESS.SIZE'='8192')
;

SELECT "Creating temp specguide_worldbox_activity table if it does not exist - not partitioned";

-- Create temp table to hold VOD and Netflix data for WorldBoxes
-- CREATE TABLE IF NOT EXISTS ${env:TMP_db}.specguide_worldbox_activity
DROP TABLE dev_tmp.specguide_worldbox_activity;
CREATE TABLE IF NOT EXISTS dev_tmp.specguide_worldbox_activity
  (
  account__mac_id_aes256_standard string,
  account__number_aes256 string,
  account__type string,
  customer__type string,
  application string,
  application_subtype string,
  source_application_name string,
  message__name string,
  data_source string,
  partition_date_denver string,
  load_date string
  )
STORED AS ORC
TBLPROPERTIES (
 'ORC.COMPRESS'='SNAPPY',
 'ORC.COMPRESS.SIZE'='8192')
;



SELECT "Creating temp specguide_worldbox_metrics table if it does not exist";

-- CREATE TABLE IF NOT EXISTS ${env:TMP_db}.specguide_worldbox_metrics
DROP TABLE dev_tmp.specguide_worldbox_metrics;
CREATE TABLE IF NOT EXISTS dev_tmp.specguide_worldbox_metrics
(
partition_date_denver string,
analysis_period string,
analysis_period_end_date string,
account_type string,
metric_data_source string,
grouping_id int,
metric_name string,
metric_value double
)
STORED AS ORC
TBLPROPERTIES (
 'ORC.COMPRESS'='SNAPPY',
 'ORC.COMPRESS.SIZE'='8192')
;


SELECT "Creating specguide_worldbox_metrics table if it does not exist";

-- CREATE TABLE IF NOT EXISTS specguide_worldbox_metrics
DROP TABLE dev.specguide_worldbox_metrics;
CREATE TABLE IF NOT EXISTS dev.specguide_worldbox_metrics

(
analysis_period string,
analysis_period_end_date string,
account_type string,
metric_data_source string,
grouping_id int,
metric_name string,
metric_value double,
date_loaded string
)
PARTITIONED BY (partition_date_denver string)
STORED AS ORC
TBLPROPERTIES (
 'ORC.COMPRESS'='SNAPPY',
 'ORC.COMPRESS.SIZE'='8192')
;
