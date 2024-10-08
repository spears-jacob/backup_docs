set hive.vectorized.execution.enabled = false;

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
SET pivot=asp_metric_pivot_smb_quantum;
SET ts_multiplier=1;

SELECT'

***** -- Dynamic Variables -- *****
';
SET END_DATE_utc=${hiveconf:END_DATE_ne};
SET dt_hr=_06;
SET alias=al;

SELECT'
--------------------------------------------------------------------------------
-------------------------- ***** Table Cleanup ***** ---------------------------
--------------------------------------------------------------------------------

';

TRUNCATE TABLE ${hiveconf:pivot} PARTITION(platform,domain,company,data_source,year_fiscal_month);
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_smb_quantum1 PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_smb_quantum2 PURGE;

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

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_smb_quantum1 AS
SELECT
  '${hiveconf:chtr}' as company,

-- begin definitions

SIZE(COLLECT_SET(visit__visit_id))
  AS web_sessions_visits,
SUM(IF(LOWER(visit__application_details__application_name)= LOWER('SMB')
   AND LOWER(message__name) = LOWER('pageView')
   AND LOWER(state__view__current_page__app_section) = LOWER('support'), 1, 0))
  AS support_section_page_views,
SIZE(COLLECT_SET(IF(visit__application_details__application_name = 'SMB'
    AND message__name = 'selectAction'
    AND state__view__current_page__elements__standardized_name IN('downloadStatement', 'pay-bill.billing-statement-download')),1,0))
  AS view_statements,
SIZE(COLLECT_SET(IF(visit__application_details__application_name = 'SMB'
    AND message__name = 'featureStop'
    AND ((message__feature__feature_step_name IN('otpSuccessAutoPay')
      AND message__feature__feature_step_changed = TRUE)
      OR (message__feature__feature_name = 'oneTimeBillPayFlow'
        AND operation__success = TRUE
        AND message__feature__feature_step_name IN('oneTimePaymentAutoPayAppSuccess')
        AND message__feature__feature_step_changed = TRUE))), 1, 0))
  AS one_time_payments,
SIZE(COLLECT_SET(IF(visit__application_details__application_name = 'SMB'
    AND ((message__name = 'featureStop'
    AND message__feature__feature_step_name IN( 'apEnrollSuccessWithPayment')
    AND message__feature__feature_step_changed = TRUE)
      OR ((message__event_case_id IN ('SPECNET_billPay_billPayStop_autopayEnrollmentWithPayment_triggeredByApplicationSuccess')))), 1, 0)))
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
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name)= LOWER('SMB')
    AND LOWER(message__name) = LOWER('pageView')
    AND LOWER(state__view__current_page__page_name) = LOWER('recoveryEmailSent'), visit__visit_id, NULL)))
  AS id_recovery_attempts,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SMB')
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
  fiscal_month AS year_fiscal_month
FROM prod.${hiveconf:source_table1} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
  ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_utc} >= '${env:START_DATE_TZ}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_utc} <  '${env:END_DATE_TZ}')
GROUP BY
fiscal_month,
'${hiveconf:chtr}'
;

SELECT'
--------------------------------------------------------------------------------
----------------------------- ***** Quantum2 ***** -----------------------------
--------------------------------------------------------------------------------
';

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
          FROM ${env:TMP_db}.asp_metric_pull_smb_quantum1
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
----------------------------- ***** Quantum2 ***** -----------------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
--------------------- ***** END SMB Quantum Metrics ***** ---------------------
--------------------------------------------------------------------------------

';
