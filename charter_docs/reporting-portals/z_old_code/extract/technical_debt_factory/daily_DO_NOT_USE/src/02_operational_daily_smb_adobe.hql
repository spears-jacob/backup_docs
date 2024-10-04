set hive.vectorized.execution.enabled = false;
set hive.auto.convert.join=false;
set tez.am.resource.memory.mb=12288;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
--------------------------- ***** SMB ADOBE ***** ------------------------------
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
SET partition_dt_den=date_denver;
SET source=Adobe;
SET company=All_Companies;
SET domain=smb;
SET source_table1=asp_v_bounces_entries;
SET source_table_fid=asp_v_federated_identity;
SET ts_multiplier=1000;

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

TRUNCATE TABLE asp_metric_pivot_smb_adobe_daily PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm});
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_smb_adobe_daily PURGE;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
----------------------------- *** Bounce Rate *** ------------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_smb_adobe_daily AS
SELECT
'${hiveconf:company}' as company,
-- begin definitions

entries as homeunauth_entries,
bounces as homeunauth_bounces,

-- end definitions

'asp' AS platform,
'${hiveconf:domain}' AS domain,
'${hiveconf:source}' AS data_source,
'${env:PART_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table1} ${hiveconf:alias}
wHERE (${hiveconf:alias}.${hiveconf:partition_dt_den} >= '${env:START_DATE_TZ}'
AND ${hiveconf:alias}.${hiveconf:partition_dt_den} <  '${env:END_DATE_TZ}')
AND domain = 'smb'
AND page_name='home-unauth'
;

SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------
';

INSERT INTO TABLE asp_metric_pivot_resi_adobe_daily
PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm})

    SELECT
      metric_value,
      metric,
      review_comment,
      additional_comment,
      platform,
      domain,
      company,
      data_source,
      ${hiveconf:partition_nm}
    FROM (SELECT
          platform,
          domain,
          company,
          'Not a review metric' as review_comment,
          'No additional comments' as additional_comment,
          data_source,
          ${hiveconf:partition_nm},
          MAP(
            'homeunauth_entries',homeunauth_entries,
            'homeunauth_bounces',homeunauth_bounces
          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_smb_adobe_daily
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
------------------------------- ***** Page Load Times ***** --------------------
--------------------------------------------------------------------------------

';
--currently commiting out this section. Once the job is running in prod and existing ETL is retired this step can run.
--INSERT OVERWRITE TABLE prod.asp_operational_daily_page_views_by_page_load_time PARTITION(domain, date_Denver)

--SELECT  date_add(date_denver,1) AS ReportDay,
--        auth_unauth_page as page_name,
--        hot_pg_load_sec as hot_pg_load_sec,
--        cold_pg_load_sec as cold_pg_load_sec,
--        SUM(count_page_views) as page_views,
--        'SB.net - Venona' as domain,
--        date_denver
--FROM  (SELECT
--          date_denver,
--          CASE WHEN page_name = 'homeAuth' THEN 'home-authenticated'
--               WHEN page_name IN('homeUnauth','login','Login') THEN 'home-unauth'
--               ELSE 'other'
--          END as auth_unauth_page,
--          count_page_views,
--          hot_pg_load_sec,
--          cold_pg_load_sec
--        FROM prod.asp_v_page_render_time_seconds_page_views_visits
--        wHERE (${hiveconf:alias}.${hiveconf:partition_dt_den} >= '${env:START_DATE_TZ}'
--        AND ${hiveconf:alias}.${hiveconf:partition_dt_den} <  '${env:END_DATE_TZ}')
--            AND page_name in ('homeAuth','homeUnauth','login','Login')
--            AND domain = 'smb'
--      ) dictionary
--GROUP BY  date_denver,
--          auth_unauth_page,
--          hot_pg_load_sec,
--          cold_pg_load_sec
--;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** END SMB Adobe Metrics ***** ----------------------
--------------------------------------------------------------------------------

';
