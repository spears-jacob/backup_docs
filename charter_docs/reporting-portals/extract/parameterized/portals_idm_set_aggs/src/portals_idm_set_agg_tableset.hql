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

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.venona_set_agg_idm_stage_accounts_${env:execid}

  (
    page_name                   STRING,
    app_section                 STRING,
    user_role                   STRING,
    device_id                   STRING,
    visit_id                    STRING,
    application_type            STRING,
    device_type                 STRING,
    app_version                 STRING,
    logged_in                   STRING,
    application_name            STRING,
    os_name                     STRING,
    operating_system            STRING,
    browser_name                STRING,
    browser_version             STRING,
    browser_size_breakpoint     STRING,
    form_factor                 STRING,
    referrer_link               STRING,
    grouping_id                 INT,
    metric_name                 STRING,
    metric_value                DOUBLE,
    process_date_time_denver    STRING,
    process_identity            STRING,
    unit_type                   STRING,
    grain                       STRING,
    label_date_denver           STRING
  )
;



SELECT '
--------------------------------------------------------------------------------
---------------------***** CREATE DEVICES TEMP TABLE *****----------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.venona_set_agg_idm_stage_devices_${env:execid}

(
  page_name                   STRING,
  app_section                 STRING,
  user_role                   STRING,
  device_id                   STRING,
  visit_id                    STRING,
  application_type            STRING,
  device_type                 STRING,
  app_version                 STRING,
  logged_in                   STRING,
  application_name            STRING,
  os_name                     STRING,
  operating_system            STRING,
  browser_name                STRING,
  browser_version             STRING,
  browser_size_breakpoint     STRING,
  form_factor                 STRING,
  referrer_link               STRING,
  grouping_id                 INT,
  metric_name                 STRING,
  metric_value                DOUBLE,
  process_date_time_denver    STRING,
  process_identity            STRING,
  unit_type                   STRING,
  grain                       STRING,
  label_date_denver           STRING
)
;


SELECT '
--------------------------------------------------------------------------------
--------------------***** CREATE INSTANCES TEMP TABLE *****---------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.venona_set_agg_idm_stage_instances_${env:execid}

(
  page_name                   STRING,
  app_section                 STRING,
  user_role                   STRING,
  device_id                   STRING,
  visit_id                    STRING,
  application_type            STRING,
  device_type                 STRING,
  app_version                 STRING,
  logged_in                   STRING,
  application_name            STRING,
  os_name                     STRING,
  operating_system            STRING,
  browser_name                STRING,
  browser_version             STRING,
  browser_size_breakpoint     STRING,
  form_factor                 STRING,
  referrer_link               STRING,
  grouping_id                 INT,
  metric_name                 STRING,
  metric_value                DOUBLE,
  process_date_time_denver    STRING,
  process_identity            STRING,
  unit_type                   STRING,
  grain                       STRING,
  label_date_denver           STRING
)
;


SELECT '
--------------------------------------------------------------------------------
----------------------***** CREATE VISITS TEMP TABLE *****----------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.venona_set_agg_idm_stage_visits_${env:execid}

(
  page_name                   STRING,
  app_section                 STRING,
  user_role                   STRING,
  device_id                   STRING,
  visit_id                    STRING,
  application_type            STRING,
  device_type                 STRING,
  app_version                 STRING,
  logged_in                   STRING,
  application_name            STRING,
  os_name                     STRING,
  operating_system            STRING,
  browser_name                STRING,
  browser_version             STRING,
  browser_size_breakpoint     STRING,
  form_factor                 STRING,
  referrer_link               STRING,
  grouping_id                 INT,
  metric_name                 STRING,
  metric_value                DOUBLE,
  process_date_time_denver    STRING,
  process_identity            STRING,
  unit_type                   STRING,
  grain                       STRING,
  label_date_denver           STRING
)
;
