set hive.vectorized.execution.enabled = false;
set tez.am.resource.memory.mb=12288;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
--------------------------- ***** SMB QUANTUM ***** ----------------------------
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
SET ts=received__timestamp;
SET partition_dt_utc=partition_date_hour_utc;
SET partition_dt_fid=partition_date_denver;
SET source=Quantum;
SET chtr=CHTR;
SET bhn=BHN;
SET twc=TWC;
SET domain=smb;
SET source_table1=asp_v_venona_events_portals_smb;
SET pivot=asp_metric_pivot_smb_quantum_wk;
SET ts_multiplier=1;

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

TRUNCATE TABLE ${hiveconf:pivot} PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm});
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_smb_quantum1_wk PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_smb_quantum2_wk PURGE;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
----------------------------- ***** Quantum1 ***** -----------------------------
--------------------------------------------------------------------------------
';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_smb_quantum1_wk AS
SELECT
  '${hiveconf:chtr}' as company,

-- begin definitions

SIZE(COLLECT_SET(visit__visit_id))
  AS web_sessions_visits,
SUM(IF(LOWER(visit__application_details__application_name)= LOWER('SMB')
   AND LOWER(message__name) = LOWER('pageView')
   AND LOWER(state__view__current_page__app_section) = LOWER('support'), 1, 0))
  AS support_section_page_views,
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
   AND LOWER(message__name) = LOWER('selectAction')
   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('downloadStatement'), 1, 0))
  AS view_statements,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
    AND ((LOWER(message__name) = LOWER('featureStop'))
    OR (LOWER(message__name) = LOWER('pageView')
    AND LOWER(state__view__current_page__page_name) = LOWER('autoPayEnrollOtpSuccess'))), visit__visit_id, NULL)))
  AS one_time_payments,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
    AND LOWER(message__name) = LOWER('pageView')
    AND LOWER(state__view__current_page__page_name) = LOWER('autoPayEnrollOtpSuccess'), visit__visit_id, NULL)))
    +
    SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
      AND LOWER(message__name) = LOWER('pageView')
      AND LOWER(state__view__current_page__page_name) = LOWER('autoPayEnrollSuccess'), visit__visit_id, NULL)))
  AS set_up_auto_payments,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
    AND LOWER(message__name) = LOWER('selectAction')
    AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('addNewUserConfirm')
    AND LOWER(message__context) = LOWER('Administrator'), visit__visit_id, NULL)))
  AS id_creation_attempts,
SIZE(COLLECT_SET(CASE
  WHEN LOWER(visit__application_details__application_name) = LOWER('SMB')
    AND LOWER(message__name) = LOWER('applicationActivity')
    AND LOWER(operation__operation_type) = LOWER('userAddSuccessBanner')
    AND LOWER(message__context) = LOWER('Administrator')
  THEN visit__visit_id ELSE NULL END))
  AS ids_created,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
    AND LOWER(message__name) = LOWER('selectAction')
    AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('addNewUserConfirm')
    AND LOWER(message__context) = LOWER('Standard'), visit__visit_id, NULL)))
  AS new_sub_user_creation_attempts,
SIZE(COLLECT_SET(CASE
  WHEN LOWER(visit__application_details__application_name) = LOWER('SMB')
    AND LOWER(message__name) = LOWER('applicationActivity')
    AND LOWER(operation__operation_type) = LOWER('userAddSuccessBanner')
    AND LOWER(message__context) = LOWER('Standard')
  THEN visit__visit_id ELSE NULL END))
  AS new_sub_users_created,
SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name)= LOWER('SMB')
    AND LOWER(message__name) = LOWER('pageView')
    AND LOWER(state__view__current_page__page_name) = LOWER('recoveryEmailSent'), visit__visit_id, NULL)))
  AS id_recovery_attempts,
SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')
    AND LOWER(message__name) = LOWER('selectAction')
    AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('resetAccountPassword'), visit__visit_id, Null)))
  AS password_reset_attempts,
SIZE(COLLECT_SET(CASE
  WHEN LOWER(visit__application_details__application_name)= LOWER('SMB')
    AND LOWER(message__name) = LOWER('selectAction')
    AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('passwordRecoveryComplete')
  THEN visit__visit_id ELSE NULL END))
  AS password_reset_success,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
    AND LOWER(message__name) = LOWER('pageView')
    AND LOWER(state__view__current_page__page_name) = LOWER('cancelAppointmentSuccess'), visit__visit_id, NULL)))
  AS canceled_appointments,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
    AND LOWER(message__name) = LOWER('pageView')
    AND LOWER(state__view__current_page__page_name) = LOWER('rescheduleAppointmentSuccess'), visit__visit_id, NULL)))
  AS rescheduled_appointments,

-- end definitions

  'asp' AS platform,
  '${hiveconf:domain}' AS domain,
  '${hiveconf:source}' AS data_source,
  '${env:PART_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table1} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
  ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_utc} >= '${env:START_DATE_TZ}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_utc} <  '${env:END_DATE_TZ}')
GROUP BY
'${env:PART_DATE}',
'${hiveconf:chtr}'
;

SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
----------------------------- ***** Quantum1 ***** -----------------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE ${hiveconf:pivot}
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
            'id_creation_attempts',id_creation_attempts,
            'ids_created',ids_created,
            'new_sub_user_creation_attempts',new_sub_user_creation_attempts,
            'new_sub_users_created',new_sub_users_created,
            'id_recovery_attempts',id_recovery_attempts,
            'password_reset_attempts',password_reset_attempts,
            'password_reset_success',password_reset_success,
            'canceled_appointments',canceled_appointments,
            'rescheduled_appointments',rescheduled_appointments

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_smb_quantum1_wk
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
--------------------- ***** END SMB Quantum Metrics ***** ---------------------
--------------------------------------------------------------------------------

';
