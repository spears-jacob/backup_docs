USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_pvs
(
  visit__visit_id STRING,
  state__view__current_page__name STRING,
  message__timestamp STRING,
  message__category STRING
)
PARTITIONED BY
(
  partition_date STRING,
  source_table STRING
)
;

CREATE TABLE IF NOT EXISTS asp_pv_pairs
(
  visit__visit_id STRING,
  state__view__current_page__name STRING,
  message__category STRING,
  message__timestamp STRING,
  next_page STRING,
  next_timestamp STRING
)
PARTITIONED BY
(
  partition_date STRING,
  source_table STRING
)
;


CREATE TABLE IF NOT EXISTS asp_pv_duration
(
  visit__visit_id STRING,
  current_page STRING,
  next_page STRING,
  duration INT
)
PARTITIONED BY
(
  partition_date STRING,
  source_table STRING
)
;

CREATE TABLE IF NOT EXISTS asp_duration_stats (
    current_page    string,
    next_page       string,
    value           decimal(18,3)
)
partitioned BY
(
  partition_date          string,
  metric                  string,
  duration_type           string,
  domain                  string,
  source_table            string
)
;

CREATE VIEW IF NOT EXISTS asp_v_duration_stats as
  SELECT * FROM asp_duration_stats
;


CREATE TABLE IF NOT EXISTS asp_duration_stats_quantum (
    current_page    string,
    next_page       string,
    value           decimal(18,3)
)
partitioned BY
(
  partition_date          string,
  metric                  string,
  duration_type           string,
  domain                  string,
  source_table            string
)
;

CREATE TABLE if not exists asp_duration_raw(
  visit_id string,
  page_name string,
  ts_ms bigint,
  message_name string,
  message_feature_name string,
  message_feature_step_name string,
  operation__success boolean)
PARTITIONED BY(
  date_denver string,
  source_table string)
  ;

CREATE TABLE if not exists asp_duration_order(
  visit_id string,
  page_name string,
  message_name string,
  message_feature_name string,
  message_feature_step_name string,
  operation__success boolean,
  ts_ms bigint,
  next_page string,
  next_timestamp bigint)
PARTITIONED BY(
  date_denver string,
  source_table string)
;
