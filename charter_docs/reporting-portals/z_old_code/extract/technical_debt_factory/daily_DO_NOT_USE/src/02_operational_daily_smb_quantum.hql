set hive.vectorized.execution.enabled = false;
set hive.auto.convert.join=false;
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
SET company=All_Companies;
SET domain=smb;
SET source_table1=asp_v_venona_events_portals_smb;
SET pivot=asp_metric_pivot_smb_quantum_daily;
SET ts_multiplier=1;

SELECT'

***** -- Dynamic Variables -- *****
';
SET dt_hr=_06;
SET alias=al;
SET partition_nm=denver_date;

SELECT'
--------------------------------------------------------------------------------
-------------------------- ***** Table Cleanup ***** ---------------------------
--------------------------------------------------------------------------------

';

TRUNCATE TABLE asp_metric_pivot_smb_quantum_daily PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm});
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_smb_quantum_daily PURGE;

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

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_smb_quantum_daily AS
SELECT
'${hiveconf:company}' as company,
-- begin definitions
SUM(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'loginStart' , 1, 0))
  AS login_attempts,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'userConfigSet'
    AND visit__account__enc_account_number IS NOT NULL , visit__visit_id, Null)))
  AS site_unique_auth,
SUM(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'pageView'
    AND state__view__current_page__app_section = 'support' , 1, 0))
  AS support_page_views,
SUM(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'pageView' , 1, 0))
  AS search_results_clicked,
SUM(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'selectAction'
    AND state__view__current_page__elements__standardized_name = 'downloadStatement' , 1, 0))
  AS view_online_statement,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'featureStop'
    AND message__feature__feature_step_name IN ('otpSuccess','otpSuccessAutoPay'), visit__visit_id, Null)))
  AS one_time_payment_flow_successes_all,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'featureStop'
    AND message__feature__feature_step_name = 'otpSuccessAutoPay', visit__visit_id, Null)))
  AS one_time_payment_flow_successes_with_ap_enroll,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'featureStop'
    AND message__feature__feature_step_name = 'otpSuccess', visit__visit_id, Null)))
  AS one_time_payment_flow_successes_without_ap_enroll,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'featureStop'
    AND message__feature__feature_name = 'autoPayEnroll'
    AND operation__success = TRUE, visit__visit_id, Null)))
  AS auto_pay_setup_successes_all, --*
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'featureStop'
    AND message__feature__feature_step_name = 'apEnrollSuccessWithPayment', visit__visit_id, Null)))
  AS auto_pay_setup_successes_with_payment,--*
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'featureStop'
    AND message__feature__feature_step_name = 'apEnrollSuccess', visit__visit_id, Null)))
  AS auto_pay_setup_successes_without_payment, --* looks off
SUM(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'applicationActivity'
    AND operation__operation_type = 'userAddSuccessBanner'
    AND message__context = 'Administrator', 1, 0))
  AS new_admin_accounts_created,
SUM(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'applicationActivity'
    AND operation__operation_type = 'userAddSuccessBanner'
    AND message__context = 'Standard', 1, 0))
  AS sub_acct_created,
SUM(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'pageView'
    AND state__view__current_page__page_name = 'rescheduleAppointmentSuccess' , 1, 0))
  AS rescheduled_service_appointments,
SUM(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND message__name = 'pageView'
    AND state__view__current_page__page_name = 'cancelAppointmentSuccess' , 1, 0))
  AS cancelled_service_appointments,
SUM(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND visit__device__operating_system RLIKE '.*iOS|Android.*' , 1, 0))
  AS site_unique_mobile,
SUM(IF(LOWER(visit__application_details__application_name) = 'smb'
    AND visit__device__operating_system NOT RLIKE '.*iOS|Android.*' , 1, 0))
  AS site_unique_pc,

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
'${hiveconf:company}'
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

            'login_attempts',login_attempts,
            'site_unique_auth',site_unique_auth,
            'support_page_views',support_page_views,
            'search_results_clicked',search_results_clicked,
            'view_online_statement',view_online_statement,
            'one_time_payment_flow_successes_all',one_time_payment_flow_successes_all,
            'one_time_payment_flow_successes_with_ap_enroll',one_time_payment_flow_successes_with_ap_enroll,
            'one_time_payment_flow_successes_without_ap_enroll',one_time_payment_flow_successes_without_ap_enroll,
            'auto_pay_setup_successes_all',auto_pay_setup_successes_all,
            'auto_pay_setup_successes_with_payment',auto_pay_setup_successes_with_payment,
            'auto_pay_setup_successes_without_payment',auto_pay_setup_successes_without_payment,
            'new_admin_accounts_created',new_admin_accounts_created,
            'sub_acct_created',sub_acct_created,
            'rescheduled_service_appointments',rescheduled_service_appointments,
            'cancelled_service_appointments',cancelled_service_appointments,
            'site_unique_mobile',site_unique_mobile,
            'site_unique_pc',site_unique_pc
          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_smb_quantum_daily
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
--------------------- ***** END SMB Quantum Metrics ***** ---------------------
--------------------------------------------------------------------------------

';
