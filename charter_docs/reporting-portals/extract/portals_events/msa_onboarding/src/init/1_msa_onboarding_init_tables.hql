USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable_daily} (run_date string);
CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable_weekly} (run_date string);
CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable_monthly} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_weekly} VALUES('${env:RUN_DATE}');
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_monthly} VALUES('${env:RUN_DATE}');


USE ${env:ENVIRONMENT};

create table IF NOT EXISTS asp_msa_onboarding_metrics (
  mso STRING,
  device_type STRING,
  tutorial_completed STRING,
  aggregation STRING,
  metric_name STRING,
  metric_value DOUBLE,
  process_date_time_denver STRING,
  process_identity STRING
)
PARTITIONED BY (label_date_denver string, grain STRING);

create table IF NOT EXISTS asp_msa_onboarding_time (
  mso string,
  device_type string,
  tutorial_completed STRING,
  metric_type string,
  pagename string,
  number_of_item int,
  time_avg_sec double,
  time_25th_sec double,
  time_50th_sec double,
  time_75th_sec double,
  process_date_time_denver STRING,
  process_identity STRING
)
PARTITIONED BY (label_date_denver string, grain STRING);

create VIEW IF NOT EXISTS asp_v_venona_metric_agg_portals AS
SELECT *
    FROM prod.venona_metric_agg_portals;

create VIEW IF NOT EXISTS asp_v_msa_onboarding_metrics AS
SELECT *
    FROM asp_msa_onboarding_metrics;

create VIEW IF NOT EXISTS asp_v_msa_onboarding_time AS
SELECT *
    FROM asp_msa_onboarding_time;
