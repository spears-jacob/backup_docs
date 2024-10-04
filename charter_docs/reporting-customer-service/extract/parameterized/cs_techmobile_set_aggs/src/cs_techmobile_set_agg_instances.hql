USE ${env:ENVIRONMENT};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO ${env:TMP_db}.cs_techmobile_set_agg_stage_instances_${hiveconf:CLUSTER}
SELECT
  message_feature_transactionid,
  raw_order_number,
  visit_applicationdetails_appversion,
  visit_location_region,
  visit_location_regionname,
  visit_technician_techid,
  visit_technician_quadid,
  visit_device_devicetype,
  visit_device_model,
  jobName,
  message_timestamp,
  receivedDate,
  'instances' AS unit_type,
  grouping_id
  metric_name,
  metric_value,
  '${hiveconf:grain}' AS grain,
  partition_date_utc
FROM
  (
  SELECT
    '${hiveconf:partition_date_utc}' AS partition_date_utc,
    message_feature_transactionid,
    raw_order_number,
    visit_applicationdetails_appversion,
    visit_location_region,
    visit_location_regionname,
    visit_technician_techid,
    visit_technician_quadid,
    visit_device_devicetype,
    visit_device_model,
    jobName,
    message_timestamp,
    receivedDate,
    CAST(grouping__id AS INT) AS grouping_id,
      MAP(
     'error_count', SUM(error_count),
     'failed_login_count', SUM(failed_login_count),
     'job_completion_closure_count', SUM(job_completion_closure_count),
     'job_completion_feature_count', SUM(job_completion_feature_count),
     'job_start_feature_count', SUM(job_start_feature_count),
     'job_start_select_count', SUM(job_start_select_count),
     'login_events_count', SUM(login_events_count),
     'successful_login_count', SUM(successful_login_count)
    ) AS tmp_map
  FROM
    (
    SELECT
      '${hiveconf:partition_date_utc}' AS partition_date_utc,
      message_feature_transactionid,
      raw_order_number,
      visit_applicationdetails_appversion,
      visit_location_region,
      visit_location_regionname,
      visit_technician_techid,
      visit_technician_quadid,
      visit_device_devicetype,
      visit_device_model,
      jobName,
      message_timestamp,
      receivedDate,
        SUM(error_count) AS error_count,
        SUM(failed_login_count) AS failed_login_count,
        SUM(job_completion_closure_count) AS job_completion_closure_count,
        SUM(job_completion_feature_count) AS job_completion_feature_count,
        SUM(job_start_feature_count) AS job_start_feature_count,
        SUM(job_start_select_count) AS job_start_select_count,
        SUM(login_events_count) AS login_events_count,
        SUM(successful_login_count) AS successful_login_count
      FROM cs_techmobile_metric_agg
      WHERE (partition_date_utc >= ("${hiveconf:START_DATE}") AND partition_date_utc < ("${hiveconf:END_DATE}"))
      GROUP BY
      message_feature_transactionid,
      raw_order_number,
      visit_applicationdetails_appversion,
      visit_location_region,
      visit_location_regionname,
      visit_technician_techid,
      visit_technician_quadid,
      visit_device_devicetype,
      visit_device_model,
      jobName,
      message_timestamp,
      receivedDate,
      ) sumfirst
    GROUP BY
      partition_date_utc,
      message_feature_transactionid,
      raw_order_number,
      visit_applicationdetails_appversion,
      visit_location_region,
      visit_location_regionname,
      visit_technician_techid,
      visit_technician_quadid,
      visit_device_devicetype,
      visit_device_model,
      jobName,
      message_timestamp,
      receivedDate,
      unit_type
    GROUPING SETS (
      (unit_identifier),
      (unit_identifier, visit_applicationdetails_appversion),
      (unit_identifier, visit_applicationdetails_appversion, visit_technician_techid),
      (unit_identifier, visit_applicationdetails_appversion, visit_technician_techid, jobName),
      (unit_identifier, visit_applicationdetails_appversion, jobName))
    ) sets
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
      FROM cs_techmobile_metric_agg
      WHERE (partition_date_utc >= ("${hiveconf:START_DATE}") AND partition_date_utc < ("${hiveconf:END_DATE}"))
      GROUP BY
        message_feature_transactionid,
        raw_order_number,
        visit_applicationdetails_appversion,
        visit_location_region,
        visit_location_regionname,
        visit_technician_techid,
        visit_technician_quadid,
        visit_device_devicetype,
        visit_device_model,
        jobName,
        message_timestamp,
        receivedDate,
        '${env:partition_date_utc}'
      ) sumfirst
    GROUP BY
    unit_identifier,
    message_feature_transactionid,
    raw_order_number,
    visit_applicationdetails_appversion,
    visit_location_region,
    visit_location_regionname,
    visit_technician_techid,
    visit_technician_quadid,
    visit_device_devicetype,
    visit_device_model,
    jobName,
    message_timestamp,
    receivedDate,
    unit_type
  GROUPING SETS (
    (unit_identifier),
    (unit_identifier, visit_applicationdetails_appversion),
    (unit_identifier, visit_applicationdetails_appversion, visit_technician_techid),
    (unit_identifier, visit_applicationdetails_appversion, visit_technician_techid, jobName),
    (unit_identifier, visit_applicationdetails_appversion, jobName))
  ) sets
  GROUP BY
    '${hiveconf:partition_date_utc}',
    aw_order_number,
    raw_order_number,
    visit_applicationdetails_appversion,
    visit_location_region,
    visit_location_regionname,
    visit_technician_techid,
    visit_technician_quadid,
    visit_device_devicetype,
    visit_device_model,
    jobName,
    message_timestamp,
    receivedDate,
    grouping_id,
    unit_type
  ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;
