set hive.vectorized.execution.enabled = false;

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
SET chtr=CHTR;
SET bhn=BHN;
SET twc=TWC;
SET domain=resi;
SET source_table1=asp_v_venona_events_portals_specnet;
SET ts_multiplier=1;

SELECT'

***** -- Dynamic Variables -- *****
';
SET START_DATE_ne="DATE_ADD('${hiveconf:END_DATE_ne}',-6)";
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

TRUNCATE TABLE asp_metric_pivot_resi_quantum_wk PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm});
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_resi_quantum1_wk PURGE;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_resi_quantum1_wk AS

SELECT
  '${hiveconf:chtr}' as company,

-- begin definitions

SIZE(COLLECT_SET(visit__visit_id))
  AS web_sessions_visits,
SUM(IF(visit__application_details__application_name = 'SpecNet'
    AND message__name = 'pageView'
    AND state__view__current_page__app_section = 'support', 1, 0))
  AS support_section_page_views,
SUM(IF(visit__application_details__application_name = 'SpecNet'
    AND message__name = 'selectAction'
    AND state__view__current_page__elements__standardized_name = 'pay-bill.billing-statement-download', 1, 0))
  AS view_statements,
(SIZE(COLLECT_SET(IF (visit__application_details__application_name = 'SpecNet'
      AND LOWER(message__name) = LOWER('featureStop')
      AND LOWER(message__feature__feature_name) = LOWER('oneTimeBillPayFlow')
      AND LOWER(operation__success) = LOWER('True')
      AND LOWER(message__feature__feature_step_changed) = LOWER('False') , visit__visit_id, NULL)))
      +
      SIZE(COLLECT_SET(IF (visit__application_details__application_name = 'SpecNet'
        AND LOWER(message__name) = LOWER('featureStop')
        AND LOWER(message__feature__feature_name) = LOWER('oneTimeBillPayFlow')
        AND LOWER(operation__success) = LOWER('True')
        AND LOWER(message__feature__feature_step_name) = LOWER('oneTimePaymentAutoPaySuccess'), visit__visit_id, NULL))))
  AS one_time_payments,
(SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SpecNet')
      AND LOWER(message__name) = LOWER('featureStop')
      AND LOWER(message__feature__feature_name) IN (LOWER('autoPayEnrollment'),LOWER('manageAutoPay'))
      AND LOWER(operation__success) = LOWER('True')
      AND LOWER(message__feature__feature_step_name) <> LOWER('autopayChangesConfirmation'), visit__visit_id, NULL)))
      +
      SIZE(COLLECT_SET(IF (visit__application_details__application_name = 'SpecNet'
        AND LOWER(message__name) = LOWER('featureStop')
        AND LOWER(message__feature__feature_name) = LOWER('oneTimeBillPayFlow')
        AND LOWER(operation__success) = LOWER('True')
        AND LOWER(message__feature__feature_step_name) = LOWER('oneTimePaymentAutoPaySuccess'), visit__visit_id, NULL))))
  AS set_up_auto_payments,
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('SpecNet')
   AND LOWER(message__name) = LOWER('selectAction')
   AND LOWER(state__view__current_page__elements__standardized_name) = 'equipment.tv.troubleshoot', 1, 0))
  AS refresh_digital_receiver_requests,
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('SpecNet')
   AND LOWER(message__name) = LOWER('selectAction')
   AND LOWER(state__view__current_page__elements__standardized_name) = 'equipment.internet.troubleshoot', 1, 0))
  AS modem_router_resets,
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('SpecNet')
   AND LOWER(message__name) = LOWER('apiCall')
   AND LOWER(application__api__path) = LOWER('/api/pub/serviceapptedge/v1/appointments/reschedule'), 1, 0))
  AS rescheduled_appointments,
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('SpecNet')
   AND LOWER(message__name) = LOWER('apiCall')
   AND LOWER(application__api__api_name) = LOWER('serviceapptedgeV1AppointmentsCancel'), 1, 0))
  AS canceled_appointments,

-- end definitions

  'asp' AS platform,
  '${hiveconf:domain}' AS domain,
  '${hiveconf:source}' AS data_source,
  '${hiveconf:END_DATE_ne}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table1} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
  ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_utc} >= '${env:START_DATE_TZ}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_utc} <  '${env:END_DATE_TZ}')
GROUP BY
'${hiveconf:END_DATE_ne}',
'${hiveconf:chtr}'
;

SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_resi_quantum_wk
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
          'refresh_digital_receiver_requests',refresh_digital_receiver_requests,
          'modem_router_resets',modem_router_resets,
          'rescheduled_appointments',rescheduled_appointments,
          'canceled_appointments',canceled_appointments

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_resi_quantum1_wk
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
--------------------- ***** END Resi Quantum Metrics ***** ---------------------
--------------------------------------------------------------------------------

';
