set hive.vectorized.execution.enabled = false;

USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
---------------------------- ***** RESI ADOBE ***** ----------------------------
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
SET domain=resi;
SET source_table1=asp_v_bounces_entries;
SET source_table2=asp_v_bhn_residential_events;
SET source_table3=asp_v_bhn_bill_pay_events;
SET source_table4=asp_v_twc_residential_global_events;
SET source_table_fid=asp_v_federated_identity;
SET ts_multiplier=1000;

SELECT'

***** -- Dynamic Variables -- *****
';
SET START_DATE_ne="'${env:START_DATE}'";
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

TRUNCATE TABLE asp_metric_pivot_resi_adobe_wk PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm});
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_chtr_wk PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn1_wk PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn2_wk PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_twc_wk PURGE;



SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
----------------------------- ***** Bounce Rate ***** --------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_daily AS
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
  AND domain = 'resi'
  AND page_name='home-unauth'
;

SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
------------------------------- ***** ID Adobe ***** -------------------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_resi_adobe_daily
PARTITION(platform,domain,data_source,${hiveconf:partition_nm})

select
STRING(SUM(value)) as metric_value,
metric,
'asp' AS platform,
'${hiveconf:domain}' AS domain,
'${hiveconf:source}' AS data_source,
${hiveconf:partition_nm}
from asp_v_counts_daily
  wHERE (${hiveconf:alias}.${hiveconf:date_denver} >= '${env:START_DATE}'
  AND ${hiveconf:alias}.${hiveconf:date_denver} <  '${env:END_DATE_TZ}')
AND domain = 'resi'
AND metric IN (
                'new_ids_incl_sub_accts',
                'combined_credential_recoveries')
AND company = 'L-CHTR'
GROUP BY  metric,
         date_Denver

SELECT'
--------------------------------------------------------------------------------
------------------------------- ***** Bouce Rate ***** -------------------------
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
          FROM ${env:TMP_db}.asp_metric_pull_resi_daily
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;


SELECT'
--------------------------------------------------------------------------------
------------------ ***** STEP 3: One-Off Metric Inserts ***** ------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_resi_adobe_wk
PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm})
SELECT

SIZE(COLLECT_SET(IF(account_number_aes256 IS NOT NULL,account_number_aes256, NULL))) AS metric_value,
'hhs_logged_in' AS metric,
'asp' AS platform,
'${hiveconf:domain}' AS domain,
CASE WHEN footprint = 'Charter' THEN '${hiveconf:chtr}' ELSE footprint END as company,
'${hiveconf:source}' AS data_source,
'${env:END_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table_fid} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
    ON ${hiveconf:alias}.${hiveconf:partition_dt_fid} = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_fid} >= '${env:START_DATE}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_fid} <  '${env:END_DATE}')
  AND LOWER(source_app) IN('portals-idp')
  AND account_number_aes256 IS NOT NULL
  AND is_success = true
GROUP BY
'${env:END_DATE}',
CASE WHEN footprint = 'Charter' THEN '${hiveconf:chtr}' ELSE footprint END
;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** END Resi Adobe Metrics ***** ----------------------
--------------------------------------------------------------------------------

';
