set hive.vectorized.execution.enabled = false;
set tez.am.resource.memory.mb=12288;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

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
SET source_table1=asp_v_net_events;
SET source_table2=asp_v_bhn_residential_events;
SET source_table3=asp_v_bhn_bill_pay_events;
SET source_table4=asp_v_twc_residential_global_events;
SET source_table_fid=asp_v_federated_identity;
SET ts_multiplier=1000;

SELECT'

***** -- Dynamic Variables -- *****
';
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
----------------------------- ***** CHARTER ***** ------------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_chtr_wk AS
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
  '${env:PART_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table1} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
  ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
    WHERE (${hiveconf:alias}.${hiveconf:partition_dt} >= '${env:START_DATE}'
    AND ${hiveconf:alias}.${hiveconf:partition_dt} <  '${env:END_DATE}')
GROUP BY
'${env:PART_DATE}',
'${hiveconf:chtr}'
;

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** BHN ***** ---------------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn1_wk AS
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
'${env:PART_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table2} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_utc} >= '${env:START_DATE_TZ}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_utc} <  '${env:END_DATE_TZ}')
GROUP BY
'${env:PART_DATE}',
'${hiveconf:bhn}'
;

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn2_wk AS
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
  '${env:PART_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table3} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_utc} >= '${env:START_DATE_TZ}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_utc} <  '${env:END_DATE_TZ}')
GROUP BY
'${env:PART_DATE}',
'${hiveconf:bhn}'
;




SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** TWC ***** ---------------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_twc_wk AS
SELECT
  '${hiveconf:twc}' as company,

-- begin definitions

SIZE(COLLECT_SET(visit__visit_id))
  AS web_sessions_visits,
SUM(IF(LOWER(message__name) RLIKE '.*support.*'
    AND (message__category = 'Page View') , 1, 0))
  AS support_section_page_views,
SUM(IF(state__view__current_page__elements__name RLIKE 'ebpp:statement download.*'
    AND ARRAY_CONTAINS(message__feature__name,'Instance of eVar7'),1,0))
  AS view_statements,
SIZE(COLLECT_SET(IF(operation__operation_type RLIKE '.*one time.*'
    AND state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you'
    AND message__category = 'Page View', visit__visit_id, NULL)))
  AS one_time_payments,
SIZE(COLLECT_SET(IF(operation__operation_type RLIKE '.*recurring:.*'
    AND state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you'
    AND message__category = 'Page View', visit__visit_id, NULL)))
  AS set_up_auto_payments,
SUM(IF(array_contains(message__feature__name,'Custom Event 5'),1,0))
  AS ids_created,
SUM(IF(array_contains(message__feature__name,'Custom Event 1'),1,0))
  AS id_creation_attempts,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > reset password > change password confirmation',visit__visit_id,NULL)))
  AS password_reset_success,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > reset password > step 1',visit__visit_id, NULL)))
  AS password_reset_attempts,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > retrieve username > username displayed'
    AND message__category = 'Page View', visit__visit_id, NULL)))
  AS id_recovery_success,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > retrieve username > lookup account', visit__visit_id, NULL)))
  AS id_recovery_attempts,
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section RLIKE "^ats:troubleshoot:tv.*confirmation$", visit__visit_id, NULL)))
  AS refresh_digital_receiver_requests,
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section RLIKE "^ats:troubleshoot:homephone.*confirmation$"
    OR state__view__current_page__sub_section RLIKE "^ats:troubleshoot:internet.*confirmation$", visit__visit_id, NULL)))
  AS modem_router_resets,
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section = 'services : my services : appointment manager : reschedule submitted', visit__visit_id, NULL)))
  AS rescheduled_appointments,
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section = 'my services > appointment manager > cancel submitted', visit__visit_id, NULL)))
  AS canceled_appointments,

-- end definitions

'asp' AS platform,
'${hiveconf:domain}' AS domain,
'${hiveconf:source}' AS data_source,
'${env:PART_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table4} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_utc} >= '${env:START_DATE_TZ}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_utc} <  '${env:END_DATE_TZ}')
GROUP BY
'${env:PART_DATE}',
'${hiveconf:twc}'
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

INSERT INTO TABLE asp_metric_pivot_resi_adobe_wk
PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm})

    SELECT
      metric_value,
      metric,
      platform,
      domain,
      company,
      data_source,
      ${hiveconf:partition_nm}
    FROM (SELECT
          platform,
          domain,
          company,
          data_source,
          ${hiveconf:partition_nm},
          MAP(

          'ids_created',ids_created,
          'id_creation_attempts',id_creation_attempts,
          'password_reset_success',password_reset_success,
          'password_reset_attempts',password_reset_attempts,
          'id_recovery_success',id_recovery_success,
          'id_recovery_attempts',id_recovery_attempts

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_resi_adobe_chtr_wk
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
------------------------------- ***** BHN ***** --------------------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_resi_adobe_wk
PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm})

    SELECT
      metric_value,
      metric,
      platform,
      domain,
      company,
      data_source,
      ${hiveconf:partition_nm}
    FROM (SELECT
          platform,
          domain,
          company,
          data_source,
          ${hiveconf:partition_nm},
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
          FROM ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn1_wk
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

INSERT INTO TABLE asp_metric_pivot_resi_adobe_wk
PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm})

    SELECT
      metric_value,
      metric,
      platform,
      domain,
      company,
      data_source,
      ${hiveconf:partition_nm}
    FROM (SELECT
          platform,
          domain,
          company,
          data_source,
          ${hiveconf:partition_nm},
          MAP(

            'view_statements',view_statements,
            'one_time_payments',one_time_payments,
            'set_up_auto_payments',set_up_auto_payments

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_resi_adobe_bhn2_wk
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
------------------------------- ***** TWC ***** --------------------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_resi_adobe_wk
PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm})

    SELECT
      metric_value,
      metric,
      platform,
      domain,
      company,
      data_source,
      ${hiveconf:partition_nm}
    FROM (SELECT
          platform,
          domain,
          company,
          data_source,
          ${hiveconf:partition_nm},
          MAP(

            'web_sessions_visits',web_sessions_visits,
            'support_section_page_views',support_section_page_views,
            'view_statements',view_statements,
            'one_time_payments',one_time_payments,
            'set_up_auto_payments',set_up_auto_payments,
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
          FROM ${env:TMP_db}.asp_metric_pull_resi_adobe_twc_wk
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
'${env:PART_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table_fid} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
    ON ${hiveconf:alias}.${hiveconf:partition_dt_fid} = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_fid} >= '${env:START_DATE}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_fid} <  '${env:END_DATE}')
  AND LOWER(source_app) IN('portals-idp')
  AND account_number_aes256 IS NOT NULL
  AND is_success = true
GROUP BY
'${env:PART_DATE}',
CASE WHEN footprint = 'Charter' THEN '${hiveconf:chtr}' ELSE footprint END
;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** END Resi Adobe Metrics ***** ----------------------
--------------------------------------------------------------------------------

';
