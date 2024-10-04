set hive.vectorized.execution.enabled = false;

USE dev;

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
SET source_table_fid=asp_v_federated_identity;
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
-------------------------- ***** Table Cleanup ***** ---------------------------
--------------------------------------------------------------------------------

';

TRUNCATE TABLE asp_metric_pivot_resi_adobe PARTITION(platform,domain,company,data_source,year_fiscal_month);
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn1 PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn2 PURGE;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** BHN ***** ---------------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn1 AS
SELECT
  '${hiveconf:bhn}' as company,

-- begin definitions

SIZE(COLLECT_SET(IF(visit__visit_id IS NOT NULL, visit__visit_id, NULL)))
  AS web_sessions_visits,
SUM(IF(LOWER(visit__settings["post_prop6"]) = 'support active'
    AND message__category = 'Page View', 1,0))
  AS support_section_page_views,
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'registration success'
    AND message__category = 'Custom Link', visit__visit_id, NULL)))
  AS ids_created,
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'registration submit'
    AND message__category = 'Custom Link', visit__visit_id, NULL)))
  AS id_creation_attempts,
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'reset password success'
    AND message__category = 'Custom Link', visit__visit_id, NULL)))
  AS password_reset_success,
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'reset password step1'
    AND message__category = 'Custom Link', visit__visit_id, NULL)))
  AS password_reset_attempts,
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'recover username success'
    AND message__category = 'Custom Link', visit__visit_id, NULL)))
  AS id_recovery_success,
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'recover username step1'
    AND message__category = 'Custom Link', visit__visit_id, NULL)))
  AS id_recovery_attempts,
CAST(NULL AS STRING)
  AS refresh_digital_receiver_requests,
CAST(NULL AS STRING)
  AS modem_router_resets,
CAST(NULL AS STRING)
  AS rescheduled_appointments,
CAST(NULL AS STRING)
  AS canceled_appointments,

-- end definitions

'asp' AS platform,
'${hiveconf:domain}' AS domain,
'${hiveconf:source}' AS data_source,
fiscal_month AS year_fiscal_month
FROM prod.${hiveconf:source_table2} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_utc} >= ADD_MONTHS('${hiveconf:END_DATE_utc}${hiveconf:dt_hr}',-6)
  AND ${hiveconf:alias}.${hiveconf:partition_dt_utc} <  '${hiveconf:END_DATE_utc}${hiveconf:dt_hr}')
GROUP BY
fiscal_month,
'${hiveconf:bhn}'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn2 AS
SELECT
  '${hiveconf:bhn}' as company,

-- begin definitions

SUM(IF(message__name RLIKE('.*stmtdownload/.*')
    AND message__category = 'Exit Link'
    AND visit__settings["post_evar9"] = 'RES', 1, 0))
  AS view_statements,
sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 31')
    AND state__view__current_page__page_type='RES',1,0))
  AS one_time_payments,
sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 24')
    AND state__view__current_page__page_type='RES',1,0))
  AS set_up_auto_payments,

-- end definitions

  'asp' AS platform,
  '${hiveconf:domain}' AS domain,
  '${hiveconf:source}' AS data_source,
fiscal_month AS year_fiscal_month
FROM prod.${hiveconf:source_table3} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_utc} >= ADD_MONTHS('${hiveconf:END_DATE_utc}${hiveconf:dt_hr}',-6)
  AND ${hiveconf:alias}.${hiveconf:partition_dt_utc} <  '${hiveconf:END_DATE_utc}${hiveconf:dt_hr}')
GROUP BY
fiscal_month,
'${hiveconf:bhn}'
;


SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
------------------------------- ***** BHN ***** --------------------------------
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

            'web_sessions_visits',web_sessions_visits,
            'support_section_page_views',support_section_page_views,
            'ids_created',ids_created,
            'id_creation_attempts',id_creation_attempts,
            'password_reset_success',password_reset_success,
            'password_reset_attempts',password_reset_attempts,
            'id_recovery_success',id_recovery_success,
            'id_recovery_attempts',id_recovery_attempts,
            'refresh_digital_receiver_requests',refresh_digital_receiver_requests,
            'modem_router_resets',modem_router_resets,
            'rescheduled_appointments',rescheduled_appointments,
            'canceled_appointments',canceled_appointments

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn1
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

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

            'view_statements',view_statements,
            'one_time_payments',one_time_payments,
            'set_up_auto_payments',set_up_auto_payments

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn2
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** END Resi Adobe Metrics ***** ----------------------
--------------------------------------------------------------------------------

';
