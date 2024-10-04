
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
