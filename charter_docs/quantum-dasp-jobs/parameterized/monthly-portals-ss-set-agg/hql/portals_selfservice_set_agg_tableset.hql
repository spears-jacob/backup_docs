USE ${env:ENVIRONMENT};

--------------------------------------------------------------------------------
---------- *** Table Setting For Simultaneous Data Pulls By Unit *** ----------
--------------------------------------------------------------------------------

SELECT '
# --------------------------------------------------------------------------------
# ----------------***** CREATE TEMPLATE FOR FOUR TEMP TABLES *****----------------
# --------------------------------------------------------------------------------
'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid}
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
    call_count_24h            INT,
    label_date_denver         STRING,
    grain                     STRING
  )
;

SELECT '
# --------------------------------------------------------------------------------
# --------------------***** CREATE ACCOUNTS  TEMP TABLE *****---------------------
# --------------------***** CREATE DEVICES   TEMP TABLE *****---------------------
# --------------------***** CREATE INSTANCES TEMP TABLE *****---------------------
# --------------------***** CREATE VISITS    TEMP TABLE *****---------------------
# --------------------------------------------------------------------------------
'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.quantum_set_agg_portals_stage_accounts_${hiveconf:execid}_${hiveconf:stepid}  LIKE ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid};
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.quantum_set_agg_portals_stage_devices_${hiveconf:execid}_${hiveconf:stepid}   LIKE ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid};
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.quantum_set_agg_portals_stage_instances_${hiveconf:execid}_${hiveconf:stepid} LIKE ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid};
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.quantum_set_agg_portals_stage_visits_${hiveconf:execid}_${hiveconf:stepid}    LIKE ${env:TMP_db}.qsap_template_${hiveconf:execid}_${hiveconf:stepid};
