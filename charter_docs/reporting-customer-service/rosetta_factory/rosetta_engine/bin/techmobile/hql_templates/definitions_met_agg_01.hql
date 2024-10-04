USE ${env:ENVIRONMENT};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = false;
set orc.force.positional.evolution=true;

DROP TABLE IF EXISTS ${env:TMP_db}.tm_metric_agg_${env:CLUSTER} PURGE;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.tm_metric_agg_${env:CLUSTER} AS
SELECT
message.feature.transactionid as message_feature_transactionid,
CASE WHEN LENGTH(message.feature.transactionid) > 16
    THEN SUBSTR(message.feature.transactionid,1,16)
    WHEN LENGTH(message.feature.transactionid) < 16
    THEN SUBSTR(message.feature.transactionid,6,9)
  END AS raw_order_number,
visit.applicationdetails.appversion AS visit_applicationdetails_appversion,
visit.location.region AS visit_location_region,
visit.location.regionname AS visit_location_regionname,
visit.technician.techid AS visit_technician_techid,
visit.technician.quadid AS visit_technician_quadid,
visit.device.devicetype AS visit_device_devicetype,
visit.device.model AS visit_device_model,
message.feature.featureName as jobName

--------------------------------------------------------------------------------
----------------- ***** BEGIN ROSETTA-GENERATED METRICS ***** ------------------
--------------------------------------------------------------------------------
