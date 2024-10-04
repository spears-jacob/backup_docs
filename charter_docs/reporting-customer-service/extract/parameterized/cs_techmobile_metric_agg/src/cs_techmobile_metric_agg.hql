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
        SUM(IF(message.name = 'error' AND message.triggeredBy = 'application' , 1, 0)) AS error_count,
        SUM(IF(message.name = 'loginStop' AND operation.success = false , 1, 0)) AS failed_login_count,
        SUM(IF(message.name = 'selectAction' AND state.view.currentPage.pageName = 'completeJobStep3'  AND state.view.currentPage.elements.standardizedName = 'continue' , 1, 0)) AS job_completion_closure_count,
        SUM(IF(message.name = 'featureStop' AND operation.success = true  AND message.feature.featureStepName = 'jobComplete' , 1, 0)) AS job_completion_feature_count,
        SUM(IF(message.name = 'apiCall' AND application.api.apiName = 'profileStatus'  AND message.feature.featureStepName = 'jobStart'  AND operation.success = true , 1, 0)) AS job_start_feature_count,
        SUM(IF(message.name = 'apiCall' AND application.api.apiName = 'profileStatus'  AND message.feature.featureStepName = 'jobStart' , 1, 0)) AS job_start_select_count,
        SUM(IF(message.name = 'loginStop', 1, 0)) AS login_events_count,
        SUM(IF(message.name = 'loginStop' AND operation.success = true , 1, 0)) AS successful_login_count,

--------------------------------------------------------------------------------
------------------ ***** END ROSETTA-GENERATED METRICS ***** -------------------
--------------------------------------------------------------------------------

message.`timestamp` AS message_timestamp,
prod.epoch_converter(cast(receivedtimestamp as BIGINT), 'UTC') as receivedDate,
prod.epoch_converter(cast(receivedtimestamp as BIGINT), 'UTC') as partition_date_utc
FROM prod_tmp.warp_internal_quantum_finished
  WHERE (tm.partition_date = '${hiveconf:START_DATE}')
    AND tm.visit.applicationdetails.applicationname = 'TechMobile'
    AND tm.message.name IN ('selectAction', 'error','loginStop','loginStart','featureStart','featureStop','apiCall')
    AND prod.epoch_converter(cast(message.`timestamp` as BIGINT), 'UTC') = prod.epoch_converter(cast(receivedtimestamp as BIGINT), 'UTC')
group by
  prod.epoch_converter(cast(receivedtimestamp as BIGINT), 'UTC'),
  message.feature.transactionid,
  visit.applicationdetails.appversion,
  visit.location.region,
  visit.location.regionname,
  visit.technician.techid,
  visit.technician.quadid,
  visit.device.devicetype,
  visit.device.model,
  message.feature.featureName
;

INSERT OVERWRITE TABLE cs_techmobile_metric_agg PARTITION (partition_date_utc)
select * from ${env:TMP_db}.tm_metric_agg_${env:CLUSTER};

DROP TABLE IF EXISTS ${env:TMP_db}.tm_metric_agg_${env:CLUSTER} PURGE;
