USE ${env:TMP_db};

--------------------------------------------------------------------------------
---------- *** Table Setting For Simultaneous Data Pulls By Unit *** ----------
--------------------------------------------------------------------------------

SELECT '
--------------------------------------------------------------------------------
---------------------***** CREATE ACCOUNTS TEMP TABLE *****---------------------
--------------------------------------------------------------------------------

'
;
CREATE TABLE IF NOT EXISTS ${env:domain}_${env:project}_set_agg_stage_accounts_${env:execid}
(
  app_section        STRING,
  user_role          STRING,
  message_context    STRING,
  unit_type          STRING,
  metric_name        STRING,
  metric_value       DOUBLE,
  partition_date_utc STRING,
  grain              STRING)
TBLPROPERTIES ('retention_policy'='delete immediately after running')
;


SELECT '
--------------------------------------------------------------------------------
---------------------***** CREATE DEVICES TEMP TABLE *****----------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:domain}_${env:project}_set_agg_stage_devices_${env:execid}
LIKE ${env:domain}_${env:project}_set_agg_stage_accounts_${env:execid}
;

SELECT '
--------------------------------------------------------------------------------
--------------------***** CREATE INSTANCES TEMP TABLE *****---------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:domain}_${env:project}_set_agg_stage_instances_${env:execid}
LIKE ${env:domain}_${env:project}_set_agg_stage_accounts_${env:execid}
;

SELECT '
--------------------------------------------------------------------------------
----------------------***** CREATE VISITS TEMP TABLE *****----------------------
--------------------------------------------------------------------------------

'
;

CREATE TABLE IF NOT EXISTS ${env:domain}_${env:project}_set_agg_stage_visits_${env:execid}
LIKE ${env:domain}_${env:project}_set_agg_stage_accounts_${env:execid}
;
