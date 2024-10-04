USE ${env:TMP_db};

CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable_daily} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');


USE ccpa;

--TABLE A/Grouping 1
CREATE TABLE IF NOT EXISTS asp_usage_ccpa_daily_table_a
(
  account STRING,
  billing_division_id STRING,
  billing_division STRING,
  company STRING,
  app_name STRING,
  app_type STRING,
  session_count BIGINT,
  session_duration DOUBLE,
  device_count BIGINT
)
PARTITIONED BY (denver_date STRING)
;

--TABLE B/Grouping 2
CREATE TABLE IF NOT EXISTS asp_usage_ccpa_daily_table_b
(
  account STRING,
  billing_division_id STRING,
  billing_division STRING,
  company STRING,
  app_name STRING,
  app_type STRING,
  event_type STRING,
  event_name STRING,
  event_count BIGINT
)
PARTITIONED BY (denver_date STRING)
;

--TABLE C/Grouping 3   MySpectrum Only
CREATE TABLE IF NOT EXISTS asp_usage_ccpa_daily_table_c
(
  account STRING,
  billing_division_id STRING,
  billing_division STRING,
  company STRING,
  app_name STRING,
  app_type STRING,
  device_remove_count BIGINT,
  device_add_count BIGINT,
  network_name_change BIGINT
)
PARTITIONED BY (denver_date STRING)
;
