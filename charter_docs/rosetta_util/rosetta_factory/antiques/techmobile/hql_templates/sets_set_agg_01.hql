USE ${env:ENVIRONMENT};
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO ${env:TMP_db}.cs_techmobile_set_agg_stage_{unit_type}_${hiveconf:CLUSTER}
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
  unit_type,
  grouping_id,
  metric_name,
  metric_value,
  '${hiveconf:grain}' AS grain,
  partition_date_utc
FROM
  (
  SELECT
    '${hiveconf:partition_date_utc}' AS partition_date_utc,
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
    unit_type,
    MAP(
