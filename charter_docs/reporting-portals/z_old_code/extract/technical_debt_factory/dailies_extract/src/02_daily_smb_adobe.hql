set hive.vectorized.execution.enabled = false;

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
SET partition_dt_utc=partition_date_hour_utc;
SET partition_dt_fid=partition_date_denver;
SET source=Adobe;
SET chtr=CHTR;
SET bhn=BHN;
SET twc=TWC;
SET domain=smb;
SET source_table1=asp_v_bounces_entries;
--SET source_table2=asp_v_bhn_bill_pay_events;
--SET source_table3=asp_v_twc_bus_global_events;
SET source_table4=;
SET source_table_fid=asp_v_federated_identity;
SET pivot=asp_metric_pivot_smb_adobe_daily;
SET ts_multiplier=1000;

SELECT'

***** -- Dynamic Variables -- *****
';
SET START_DATE_ne="DATE_ADD('${env:END_DATE}',-6)";
SET START_DATE_utc="DATE_ADD('${hiveconf:END_DATE_utc}',-6)";
SET END_DATE_utc=${hiveconf:END_DATE_ne};
SET START_DATE_utc="DATE_ADD('${hiveconf:END_DATE_utc}',-6)";
SET dt_hr=_06;
SET alias=al;
SET partition_nm=week_end_dt;

SELECT'
--------------------------------------------------------------------------------
-------------------------- ***** Table Cleanup ***** ---------------------------
--------------------------------------------------------------------------------

';

TRUNCATE TABLE ${hiveconf:pivot} PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm});
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
-- begin definitions

entries as homeunauth_entries,
bounces as homeunauth_bounces,

-- end definitions

'asp' AS platform,
'${hiveconf:domain}' AS domain,
'${hiveconf:source}' AS data_source,
'${env:END_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table1} ${hiveconf:alias}
wHERE (${hiveconf:alias}.${hiveconf:partition_dt_fid} >= '${env:START_DATE}'
AND ${hiveconf:alias}.${hiveconf:partition_dt_fid} <  '${env:END_DATE_TZ}')
AND domain = 'smb'
AND page_name='home-unauth'
;

SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
------------------------------- ***** Bounce Rate ***** ------------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE ${hiveconf:pivot}
PARTITION(platform,domain,data_source,${hiveconf:partition_nm})

    SELECT
      metric_value,
      metric,
      platform,
      domain,
      data_source,
      ${hiveconf:partition_nm}
    FROM (SELECT
          platform,
          domain,
          data_source,
          ${hiveconf:partition_nm},
          MAP(
            'homeunauth_entries',homeunauth_entries,
            'homeunauth_bounces',homeunauth_bounces,
          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_smb_adobe_daily
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** END SMB Adobe Metrics ***** ----------------------
--------------------------------------------------------------------------------

';
