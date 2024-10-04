set hive.vectorized.execution.enabled = false;

USE dev;

SELECT'
--------------------------------------------------------------------------------
---------------------------- ***** RESI ADOBE ***** ----------------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
-------------------------- ***** Table Cleanup ***** ---------------------------
--------------------------------------------------------------------------------

';

TRUNCATE TABLE asp_metric_pivot_resi_adobe PARTITION(platform,domain,company,data_source,year_fiscal_month);
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_chtr PURGE;

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
SET END_DATE_ne=2018-12-22;
SET ts=message__timestamp;
SET partition_dt=partition_date;
SET partition_dt_utc=partition_date_hour_utc;
SET partition_dt_fid=partition_date_denver;
SET source=Adobe;
SET chtr=CHTR;
SET bhn=BHN;
SET twc=TWC;
SET domain=resi;
SET source_table1=asp_v_net_events;
SET source_table2=asp_v_bhn_residential_events;
SET source_table3=asp_v_bhn_bill_pay_events;
SET source_table4=asp_v_twc_residential_global_events;
SET source_table_fid=federated_identity;
SET ts_multiplier=1000;

SELECT'

***** -- Dynamic Variables -- *****
';
SET START_DATE_ne="ADD_MONTHS('${hiveconf:END_DATE_ne}',-6)";
SET START_DATE_utc="ADD_MONTHS('${hiveconf:END_DATE_utc}',-6)";
SET END_DATE_utc=${hiveconf:END_DATE_ne};
SET START_DATE_utc="ADD_MONTHS('${hiveconf:END_DATE_utc}',-6)";
SET dt_hr=_06;
SET alias=al;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
----------------------------- ***** CHARTER ***** ------------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_chtr AS
SELECT
  '${hiveconf:chtr}' as company,

-- begin definitions

SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.bam','my-account.create-id-final.btm')
    AND message__category = 'Page View', visit__visit_id,NULL))) +
    SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.nbtm')
    AND message__category = 'Page View', visit__visit_id,NULL)))
  AS ids_created,
SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-1.bam','my-account.create-id')
    AND message__category = 'Page View', visit__visit_id,NULL))) -
    SIZE(COLLECT_SET(IF(message__name = 'Sign-In-Now'
    AND message__category = 'Custom Link'
    AND state__view__current_page__name IN('my-account.create-id-2.btm','my-account.create-id-2.bam'), visit__visit_id,NULL))) +
    SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-1.nbtm')
    AND message__category = 'Page View', visit__visit_id,NULL))) -
    SIZE(COLLECT_SET(IF(message__name = 'Sign-In-Now'
    AND message__category = 'Custom Link'
    AND state__view__current_page__name IN('my-account.create-id-2.nbtm'), visit__visit_id,NULL))) -
    SIZE(COLLECT_SET(IF(state__view__current_page__name IN('Create.redirectTWC')
    AND message__category = 'Page View', visit__visit_id,NULL))) -
    SIZE(COLLECT_SET(IF(state__view__current_page__name IN('Create.redirectBHN')
    AND message__category = 'Page View', visit__visit_id,NULL)))
  AS id_creation_attempts,
SIZE(COLLECT_SET(IF(message__category = 'Page View'
    AND state__view__current_page__name IN ('Reset-final.bam','Reset-final.btm')
    OR (message__category = 'Page View' AND state__view__current_page__name = 'Reset-final.nbtm'), visit__visit_id, NULL)))
  AS password_reset_success,
SIZE(COLLECT_SET(IF(message__category = 'Page View'
    AND state__view__current_page__name IN('Reset-1.bam','Reset-1.btm')
    OR (message__category = 'Page View'
    AND state__view__current_page__name = 'Reset-1.nbtm'), visit__visit_id, NULL)))
  AS password_reset_attempts,
SIZE(COLLECT_SET(if((state__view__current_page__name IN('Recover-final2.btm','Recover-final1.bam','Recover-final2.bam')
    AND message__category = 'Page View'),visit__visit_id,NULL))) +
    SIZE(COLLECT_SET(if((state__view__current_page__name = 'Recover-final1.nbtm')
    AND (message__category = 'Page View'), visit__visit_id,NULL))) +
    SIZE(COLLECT_SET(IF(state__view__current_page__name = 'Recover-final2.nbtm'
    AND (message__category = 'Page View'), visit__visit_id,NULL)))
  AS id_recovery_success,
SIZE(COLLECT_SET(IF(message__category = 'Page View'
    AND state__view__current_page__name IN('Recover-1.btm','Recover-1.bam')
    OR  message__category = 'Page View'
    AND state__view__current_page__name = 'Recover-1.nbtm'
    AND state__view__current_page__name NOT IN  ('Recover-noID.nbtm'), visit__visit_id, NULL)))
  AS id_recovery_attempts,

-- end definitions

  'asp' AS platform,
  '${hiveconf:domain}' AS domain,
  '${hiveconf:source}' AS data_source,
  fiscal_month AS year_fiscal_month
FROM prod.${hiveconf:source_table1} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
  ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
    WHERE (${hiveconf:alias}.${hiveconf:partition_dt} >= ADD_MONTHS('${hiveconf:END_DATE_ne}',-6)
    AND ${hiveconf:alias}.${hiveconf:partition_dt} <  '${hiveconf:END_DATE_ne}')
GROUP BY
fiscal_month,
'${hiveconf:chtr}'
;

SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
------------------------------- ***** CHTR ***** -------------------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_resi_adobe
PARTITION(platform,domain,company,data_source,year_fiscal_month)

    SELECT
      metric_value,
      metric,
      platform,
      domain,
      company,
      data_source,
      year_fiscal_month
    FROM (SELECT
          platform,
          domain,
          company,
          data_source,
          year_fiscal_month,
          MAP(

          'ids_created',ids_created,
          'id_creation_attempts',id_creation_attempts,
          'password_reset_success',password_reset_success,
          'password_reset_attempts',password_reset_attempts,
          'id_recovery_success',id_recovery_success,
          'id_recovery_attempts',id_recovery_attempts

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_resi_adobe_chtr
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
------------------ ***** STEP 3: One-Off Metric Inserts ***** ------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_resi_adobe
PARTITION(platform,domain,company,data_source,year_fiscal_month)
SELECT

SIZE(COLLECT_SET(IF(account_number_aes256 IS NOT NULL,account_number_aes256, NULL))) AS metric_value,
'hhs_logged_in' AS metric,
'asp' AS platform,
'${hiveconf:domain}' AS domain,
CASE WHEN footprint = 'Charter' THEN '${hiveconf:chtr}' ELSE footprint END as company,
'${hiveconf:source}' AS data_source,
fiscal_month AS year_fiscal_month
FROM prod.${hiveconf:source_table_fid} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
    ON ${hiveconf:alias}.${hiveconf:partition_dt_fid} = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_fid} >= ADD_MONTHS('${hiveconf:END_DATE_ne}',-6)
  AND ${hiveconf:alias}.${hiveconf:partition_dt_fid} <  '${hiveconf:END_DATE_ne}')
  AND LOWER(source_app) IN('portals-idp')
  AND account_number_aes256 IS NOT NULL
  AND is_success = true
GROUP BY
fiscal_month,
CASE WHEN footprint = 'Charter' THEN '${hiveconf:chtr}' ELSE footprint END
;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** END Resi Adobe Metrics ***** ----------------------
--------------------------------------------------------------------------------

';
