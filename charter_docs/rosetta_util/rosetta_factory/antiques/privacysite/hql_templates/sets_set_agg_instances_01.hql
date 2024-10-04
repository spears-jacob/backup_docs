USE ${env:ENVIRONMENT};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT INTO ${env:TMP_db}.portals_privacysite_set_agg_stage_{unit_type}_${hiveconf:CLUSTER}
SELECT
 app_section,
 user_role,
 message_context,
 '{unit_type}' AS unit_type,
 metric_name,
 metric_value,
 partition_date_utc,
 '${hiveconf:grain}' AS grain
FROM
  (
  SELECT
    '${hiveconf:partition_date_utc}' AS partition_date_utc,
    app_section,
    user_role,
    message_context,
      MAP(
