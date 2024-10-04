set hive.vectorized.execution.enabled = false;
set hive.auto.convert.join=false;
set tez.am.resource.memory.mb=12288;
set tez.am.resource.memory.mb=12288;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
---------------------------- ***** APP ADOBE ***** -----------------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
-------------------------- ***** Set Variables ***** ---------------------------
--------------------------------------------------------------------------------

--Manual Update Variables--
  ---ts=message__timestamp --received__timestamp for Quantum, message__timestamp for Adobe
  ---partition_dt=partition_date --partition_date for net_events, partition_date_hour_utc for utc tables
  ---source=Adobe --Adobe or Quantum
  ---ts_multiplier -- 1000 for Adobe 1 for Quantum

*****
';

SELECT'

***** -- Manual Variables -- *****
';
SET END_DATE_ne=${env:RUN_DATE};
SET ts=message__timestamp;
SET partition_dt=date_denver;
SET source=Adobe;
SET company=All_Companies;
SET source_table1=asp_v_app_daily_app_figures;
SET source_table2=asp_v_app_daily_app_figures_reviews;
SET ts_multiplier=1000;
SET domain=app;

SELECT'

***** -- Dynamic Variables -- *****
';
SET END_DATE_utc=${hiveconf:END_DATE_ne};
SET dt_hr=_06;
SET alias=al;
SET partition_nm=denver_date;

SELECT'
--------------------------------------------------------------------------------
-------------------------- ***** Table Cleanup ***** ---------------------------
--------------------------------------------------------------------------------

';

TRUNCATE TABLE asp_metric_pivot_app_adobe_daily PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm});

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
------------------ ***** STEP 2 : App Downloads and Updates ***** --------------
--------------------------------------------------------------------------------
';

INSERT INTO TABLE asp_metric_pivot_app_adobe_daily
PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm})

SELECT
VALUE as metric_value,
CASE WHEN ${hiveconf:alias}.METRIC = 'App Downloads' THEN 'app_downloads'
WHEN ${hiveconf:alias}.METRIC = 'App Downloads Android' THEN 'app_downloads_android'
WHEN ${hiveconf:alias}.METRIC = 'App Downloads iOS' THEN 'app_downloads_ios'
WHEN ${hiveconf:alias}.METRIC = 'App Updates' THEN 'app_updates'
WHEN ${hiveconf:alias}.METRIC = 'App Updates Android' THEN 'app_updates_android'
WHEN ${hiveconf:alias}.METRIC = 'App Updates iOS' THEN 'app_updates_ios'
else ${hiveconf:alias}.metric end as METRIC,
'Not a review metric' as review_comment,
'No additional comments' as additional_comment,
'asp' AS platform,
'${hiveconf:domain}' AS domain,
'${hiveconf:company}' as company,
'App Figures' AS data_source,
'${env:PART_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table1} ${hiveconf:alias}
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt} >= '${env:START_DATE_TZ}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt} <  '${env:END_DATE_TZ}')
  AND metric IN ('App Downloads','App Downloads Android','App Downloads iOS','App Updates','App Updates Android','App Updates iOS')
  and company = 'CHTR';

  SELECT'
  --------------------------------------------------------------------------------
  ------------------ ***** STEP 2 : App Reviews ***** ----------------------------
  --------------------------------------------------------------------------------
  ';

  INSERT INTO TABLE asp_metric_pivot_app_adobe_daily
  PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm})

  SELECT
  stars as metric_value,
  'app_review' as metric,
    review as review_comment,
  platform AS additional_comment,
  'asp' AS platform,
  '${hiveconf:domain}' AS domain,
  '${hiveconf:company}' as company,
  'App Figures' AS data_source,
  '${env:PART_DATE}' AS ${hiveconf:partition_nm}
  FROM PROD.${hiveconf:source_table2} ${hiveconf:alias}
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt} >= '${env:START_DATE_TZ}'
    AND ${hiveconf:alias}.${hiveconf:partition_dt} <  '${env:END_DATE_TZ}')
  AND company = 'CHTR';

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** END App Adobe Metrics ***** -----------------------
--------------------------------------------------------------------------------

';
