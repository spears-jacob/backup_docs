USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable_daily} (run_date STRING);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');



USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_privacysite_metric_agg
(
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
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
  error_count INT,
  failed_login_count INT,
  job_completion_closure_count INT,
  job_completion_feature_count INT,
  job_start_feature_count INT,
  job_start_select_count INT,
  login_events_count INT,
  successful_login_count INT,


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
message_timestamp,
receivedDate
)
PARTITIONED BY (partition_date_utc STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - With PII (3 Years)')
;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
