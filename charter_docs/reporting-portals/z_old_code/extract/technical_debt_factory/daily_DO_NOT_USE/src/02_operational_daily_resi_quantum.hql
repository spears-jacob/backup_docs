--set hive.vectorized.execution.enabled = false;
set hive.auto.convert.join=false;
set tez.am.resource.memory.mb=12288;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
--------------------------- ***** RESI QUANTUM ***** ---------------------------
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
SET domain=resi;
SET source_table1=asp_v_venona_events_portals_specnet;
SET ts_multiplier=1;

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

TRUNCATE TABLE asp_metric_pivot_resi_quantum_daily PARTITION(platform,domain,data_source,${hiveconf:partition_nm});
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_resi_quantum_daily PURGE;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_resi_quantum_daily AS
SELECT
  '${hiveconf:company}' as company,
-- begin definitions
SUM(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'loginStart' , 1, 0))
  AS login_attempts,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'loginStop'
    AND operation__success = 'True' , visit__visit_id, Null)))
  AS site_unique_auth,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'selectAction'
    AND state__view__current_page__elements__standardized_name IN ('equipment.internet.check-email','utilityNav-email'),visit__visit_id,Null)))
  AS webmail_views,
SUM(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'pageView'
    AND state__view__current_page__app_section = 'support' , 1, 0))
  AS support_section_page_views,
SUM(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'selectAction'
    AND operation__operation_type = 'searchResultSelected' , 1, 0))
  AS search_results_clicked,
SUM(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'selectAction'
    AND state__view__current_page__elements__standardized_name IN ('ask-spectrum','askSpectrumNextIT','askSpectrumASAPP') , 1, 0))
  AS iva_opens,
SUM(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'selectAction'
    AND state__view__current_page__elements__standardized_name = 'pay-bill.billing-statement-download' , 1, 0))
  AS view_online_statement,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'featureStop'
    AND message__feature__feature_name = 'oneTimeBillPayFlow'
    AND operation__success = TRUE
    AND (message__feature__feature_step_changed = FALSE OR message__feature__feature_step_name IN('oneTimePaymentAppSuccess','oneTimePaymentAutoPayAppSuccess')) , visit__visit_id, Null)))
  AS one_time_payment_flow_successes_all,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'featureStop'
    AND message__feature__feature_name = 'oneTimeBillPayFlow'
    AND operation__success = TRUE
    AND message__feature__feature_step_name = 'oneTimePaymentAutoPayAppSuccess' , visit__visit_id, Null)))
  AS one_time_payment_flow_successes_with_ap_enroll,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'featureStop'
    AND message__feature__feature_name = 'oneTimeBillPayFlow'
    AND operation__success = TRUE
    AND message__feature__feature_step_name = 'oneTimePaymentAppSuccess', visit__visit_id, Null)))
  AS one_time_payment_flow_successes_without_ap_enroll,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__event_case_id IN ('SPECNET_billPay_billPayStop_autopayEnrolled_triggeredByApplicationSuccess', 'SPECNET_billPay_billPayStop_autopayEnrollmentWithPayment_triggeredByApplicationSuccess') , visit__visit_id, Null))) AS set_up_auto_payment_flow_successes_all,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__event_case_id = 'SPECNET_billPay_billPayStop_autopayEnrollmentWithPayment_triggeredByApplicationSuccess' , visit__visit_id, Null)))
  AS set_up_auto_payment_flow_successes_with_payment,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__event_case_id = 'SPECNET_billPay_billPayStop_autopayEnrolled_triggeredByApplicationSuccess', visit__visit_id, Null)))
  AS set_up_auto_payment_flow_successes_without_payment,
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('SpecNet')
   AND message__name = 'featureStart'
   AND message__feature__feature_name = 'tvTroubleshoot'
   AND message__feature__feature_step_name = 'openEquipmentReset', 1, 0))
  AS refresh_digital_receiver_requests,
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('SpecNet')
   AND message__name = 'modalView'
   AND message__feature__feature_name = 'internetTroubleshoot'
   AND message__feature__feature_step_name = 'resetSuccessModal'
   AND state__view__modal__name = 'Reset-Equipment-Success', 1, 0))
  AS modem_router_resets,
SUM(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'apiCall'
    AND application__api__path = '/api/pub/serviceapptedge/v1/appointments/reschedule' , 1, 0))
  AS rescheduled_service_appointments,
SUM(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND message__name = 'apiCall'
    AND application__api__api_name = 'serviceapptedgeV1AppointmentsCancel' , 1, 0))
  AS cancelled_service_appointments,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND visit__device__operating_system RLIKE '.*iOS|Android.*' , visit__visit_id, Null)))
  AS site_unique_mobile,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'specnet'
    AND visit__device__operating_system NOT RLIKE '.*iOS|Android.*' , visit__visit_id, Null)))
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

INSERT INTO TABLE asp_metric_pivot_resi_quantum_daily
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
          'webmail_views',webmail_views,
          'support_section_page_views',support_section_page_views,
          'search_results_clicked',search_results_clicked,
          'iva_opens',iva_opens,
          'view_online_statement',view_online_statement,
          'one_time_payment_flow_successes_all',one_time_payment_flow_successes_all,
          'one_time_payment_flow_successes_with_ap_enroll',one_time_payment_flow_successes_with_ap_enroll,
          'one_time_payment_flow_successes_without_ap_enroll',one_time_payment_flow_successes_without_ap_enroll,
          'set_up_auto_payment_flow_successes_all',set_up_auto_payment_flow_successes_all,
          'set_up_auto_payment_flow_successes_with_payment',set_up_auto_payment_flow_successes_with_payment,
          'set_up_auto_payment_flow_successes_without_payment',set_up_auto_payment_flow_successes_without_payment,
          'refresh_digital_receiver_requests',refresh_digital_receiver_requests,
          'modem_router_resets',modem_router_resets,
          'rescheduled_service_appointments',rescheduled_service_appointments,
          'cancelled_service_appointments',cancelled_service_appointments,
          'site_unique_mobile',site_unique_mobile,
          'site_unique_pc',site_unique_pc
          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_resi_quantum_daily
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
--------------------- ***** END Resi Quantum Metrics ***** ---------------------
--------------------------------------------------------------------------------

';
