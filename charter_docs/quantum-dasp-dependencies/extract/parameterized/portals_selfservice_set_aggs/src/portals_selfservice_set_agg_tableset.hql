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

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.venona_set_agg_portals_stage_accounts_${env:execid}

  (
    mso                       STRING,
    application_type          STRING,
    device_type               STRING,
    connection_type           STRING,
    network_status            STRING,
    playback_type             STRING,
    cust_type                 STRING,
    application_group_type    STRING,
    app_version               STRING,
    grouping_id               INT,
    metric_name               STRING,
    metric_value              DOUBLE,
    logged_in_status          STRING,
    application_name          STRING,
    os_name                   STRING,
    os_version                STRING,
    browser_name              STRING,
    browser_version           STRING,
    form_factor               STRING,
    process_date_time_denver  STRING,
    process_identity          STRING,
    unit_type                 STRING,
    label_date_denver         STRING,
    grain                     STRING
  )
;



SELECT '
--------------------------------------------------------------------------------
---------------------***** CREATE DEVICES TEMP TABLE *****----------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.venona_set_agg_portals_stage_devices_${env:execid}

  (
    mso                       STRING,
    application_type          STRING,
    device_type               STRING,
    connection_type           STRING,
    network_status            STRING,
    playback_type             STRING,
    cust_type                 STRING,
    application_group_type    STRING,
    app_version               STRING,
    grouping_id               INT,
    metric_name               STRING,
    metric_value              DOUBLE,
    logged_in_status          STRING,
    application_name          STRING,
    os_name                   STRING,
    os_version                STRING,
    browser_name              STRING,
    browser_version           STRING,
    form_factor               STRING,
    process_date_time_denver  STRING,
    process_identity          STRING,
    unit_type                 STRING,
    label_date_denver         STRING,
    grain                     STRING
  )
;


SELECT '
--------------------------------------------------------------------------------
--------------------***** CREATE INSTANCES TEMP TABLE *****---------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.venona_set_agg_portals_stage_instances_${env:execid}

  (
    mso                       STRING,
    application_type          STRING,
    device_type               STRING,
    connection_type           STRING,
    network_status            STRING,
    playback_type             STRING,
    cust_type                 STRING,
    application_group_type    STRING,
    app_version               STRING,
    grouping_id               INT,
    metric_name               STRING,
    metric_value              DOUBLE,
    logged_in_status          STRING,
    application_name          STRING,
    os_name                   STRING,
    os_version                STRING,
    browser_name              STRING,
    browser_version           STRING,
    form_factor               STRING,
    process_date_time_denver  STRING,
    process_identity          STRING,
    unit_type                 STRING,
    label_date_denver         STRING,
    grain                     STRING
  )
;


SELECT '
--------------------------------------------------------------------------------
----------------------***** CREATE VISITS TEMP TABLE *****----------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.venona_set_agg_portals_stage_visits_${env:execid}

  (
    mso                       STRING,
    application_type          STRING,
    device_type               STRING,
    connection_type           STRING,
    network_status            STRING,
    playback_type             STRING,
    cust_type                 STRING,
    application_group_type    STRING,
    app_version               STRING,
    grouping_id               INT,
    metric_name               STRING,
    metric_value              DOUBLE,
    logged_in_status          STRING,
    application_name          STRING,
    os_name                   STRING,
    os_version                STRING,
    browser_name              STRING,
    browser_version           STRING,
    form_factor               STRING,
    process_date_time_denver  STRING,
    process_identity          STRING,
    unit_type                 STRING,
    label_date_denver         STRING,
    grain                     STRING
  )
;
