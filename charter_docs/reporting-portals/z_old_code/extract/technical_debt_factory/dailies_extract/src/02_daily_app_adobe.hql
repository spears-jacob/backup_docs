set hive.vectorized.execution.enabled = false;

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
SET partition_dt=partition_date;
SET partition_dt_utc=partition_date_hour_utc;
SET partition_dt_fid=date_denver;
SET source=Adobe;
SET chtr=CHTR;
SET bhn=BHN;
SET twc=TWC;
SET mytwc=MyTWC;
SET msa=My Spectrum;
SET domain=app;
SET source_table1=asp_v_app_daily_app_figures;
SET source_table2=asp_v_app_figures_downloads;
SET source_table_fid=asp_v_federated_identity;
SET ts_multiplier=1000;

SELECT'

***** -- Dynamic Variables -- *****
';
SET START_DATE_ne="DATE_ADD('${hiveconf:END_DATE_ne}',-6)";
SET START_DATE_utc="DATE_ADD('${hiveconf:END_DATE_utc}',-6)";
SET END_DATE_utc=${hiveconf:END_DATE_ne};
SET START_DATE_utc="DATE_ADD('${hiveconf:END_DATE_utc}',-6)";
SET dt_hr=_06;
SET alias=al;
SET partition_nm=denver_date;

SELECT'
--------------------------------------------------------------------------------
-------------------------- ***** Table Cleanup ***** ---------------------------
--------------------------------------------------------------------------------

';

TRUNCATE TABLE asp_metric_pivot_app_adobe_daily PARTITION(platform,domain,data_source,${hiveconf:partition_nm});

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
------------------ ***** STEP 2 : One-Off Metric Inserts ***** ------------------
--------------------------------------------------------------------------------
';

INSERT INTO TABLE asp_metric_pivot_app_adobe_daily
PARTITION(platform,domain,data_source,${hiveconf:partition_nm})

SELECT
VALUE as metric_value,
METRIC,
  'asp' AS platform,
  '${hiveconf:domain}' AS domain,
  'App Figures' AS data_source,
  '${hiveconf:END_DATE_ne}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table1} ${hiveconf:alias}
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_fid} >= '${env:START_DATE}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_fid} <  '${env:END_DATE_TZ}')
  AND metric IN ('App Downloads','App Downloads Android','App Downloads iOS','App Updates','App Updates Android','App Updates iOS')
  and company = 'CHTR';


SELECT'
--------------------------------------------------------------------------------
---------------------- ***** END App Adobe Metrics ***** -----------------------
--------------------------------------------------------------------------------

';
