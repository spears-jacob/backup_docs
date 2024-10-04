USE ${env:ENVIRONMENT};

--------------------------------------------------------------------------------
---------- *** Table Setting For Simultaneous Data Pulls By Unit *** ----------
--------------------------------------------------------------------------------

SELECT '
--------------------------------------------------------------------------------
---------------------***** CREATE ACCOUNTS TEMP TABLE *****---------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.cs_techmobile_set_agg_stage_accounts_${env:execid}

  (
    raw_order_number                     STRING,
    visit_applicationdetails_appversion  STRING,
    visit_location_region                STRING,
    visit_location_regionname            STRING,
    visit_technician_techid              STRING,
    visit_technician_quadid              STRING,
    visit_device_devicetype              STRING,
    visit_device_model                   STRING,
    jobName                              STRING,
    message_timestamp                    STRING,
    receivedDate                         STRING,
    unit_type                            STRING,
    grouping_id                          INT,
    metric_name                          STRING,
    metric_value                         STRING
    grain                                STRING,
    label_date_denver                    STRING
  )
;



SELECT '
--------------------------------------------------------------------------------
---------------------***** CREATE DEVICES TEMP TABLE *****----------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.cs_techmobile_set_agg_stage_devices_${env:execid}

(
  raw_order_number                     STRING,
  visit_applicationdetails_appversion  STRING,
  visit_location_region                STRING,
  visit_location_regionname            STRING,
  visit_technician_techid              STRING,
  visit_technician_quadid              STRING,
  visit_device_devicetype              STRING,
  visit_device_model                   STRING,
  jobName                              STRING,
  message_timestamp                    STRING,
  receivedDate                         STRING,
  unit_type                            STRING,
  grouping_id                          INT,
  metric_name                          STRING,
  metric_value                         STRING
  grain                                STRING,
  label_date_denver                    STRING
)
;


SELECT '
--------------------------------------------------------------------------------
--------------------***** CREATE INSTANCES TEMP TABLE *****---------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.cs_techmobile_set_agg_stage_instances_${env:execid}

(
  raw_order_number                     STRING,
  visit_applicationdetails_appversion  STRING,
  visit_location_region                STRING,
  visit_location_regionname            STRING,
  visit_technician_techid              STRING,
  visit_technician_quadid              STRING,
  visit_device_devicetype              STRING,
  visit_device_model                   STRING,
  jobName                              STRING,
  message_timestamp                    STRING,
  receivedDate                         STRING,
  unit_type                            STRING,
  grouping_id                          INT,
  metric_name                          STRING,
  metric_value                         STRING
  grain                                STRING,
  label_date_denver                    STRING
)
;


SELECT '
--------------------------------------------------------------------------------
----------------------***** CREATE VISITS TEMP TABLE *****----------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.cs_techmobile_set_agg_stage_visits_${env:execid}

(
  raw_order_number                     STRING,
  visit_applicationdetails_appversion  STRING,
  visit_location_region                STRING,
  visit_location_regionname            STRING,
  visit_technician_techid              STRING,
  visit_technician_quadid              STRING,
  visit_device_devicetype              STRING,
  visit_device_model                   STRING,
  jobName                              STRING,
  message_timestamp                    STRING,
  receivedDate                         STRING,
  unit_type                            STRING,
  grouping_id                          INT,
  metric_name                          STRING,
  metric_value                         STRING
  grain                                STRING,
  label_date_denver                    STRING
)
;
