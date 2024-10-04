set hive.vectorized.execution.enabled = false;

USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
--------------------------- ***** APP QUANTUM ***** ----------------------------
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
SET msa=My Spectrum;
SET domain=app;
SET source_table1=asp_v_venona_events_portals_msa;
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

TRUNCATE TABLE asp_metric_pivot_app_quantum PARTITION(platform,domain,company,data_source,year_fiscal_month);
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_app_quantum1 PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_app_quantum2 PURGE;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** No MSO ***** ------------------------------
--------------------------------------------------------------------------------
';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_app_quantum1 AS

SELECT
  '${hiveconf:msa}' as company,

-- begin definitions

SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('modalView')
    AND LOWER(state__view__modal__name) = LOWER('newpasswordresponse-modal')
    AND LOWER(state__view__current_page__page_name) = LOWER('newPasswordEntry'), visit__visit_id, NULL)))
  AS password_reset_success,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('selectAction')
    AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('loginRecoverySendEmail'), visit__visit_id, NULL)))
  AS id_recovery_success,

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
'${hiveconf:msa}'
;

SELECT'
--------------------------------------------------------------------------------
----------------------------- ***** With MSO ***** -----------------------------
--------------------------------------------------------------------------------
';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_app_quantum2 AS
SELECT
CASE
    WHEN LOWER(visit__account__details__mso) = LOWER('BH') THEN '${hiveconf:bhn}'
    WHEN LOWER(visit__account__details__mso) = LOWER('CHARTER') THEN '${hiveconf:chtr}'
    WHEN LOWER(visit__account__details__mso) = LOWER('TWC') THEN '${hiveconf:twc}'
    ELSE visit__account__details__mso
  END as company,

-- begin definitions

SIZE(COLLECT_SET(visit__device__enc_uuid))
  AS unique_visitors,
SIZE(COLLECT_SET(visit__visit_id))
  AS web_sessions_visits,
SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('pageView')
    AND LOWER(state__view__current_page__app_section) = LOWER('support') , 1, 0))
  AS support_section_page_views,
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('pageView')
    AND LOWER(state__view__current_page__page_name) = LOWER('statementDetail'), 1, 0))
  AS view_statements,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('pageView')
    AND LOWER(state__view__current_page__page_name)
      IN (LOWER('paySuccess'),LOWER('paySuccessAutoPay')), visit__visit_id, NULL)))
  AS one_time_payments,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('pageView')
    AND LOWER(state__view__current_page__page_name) = LOWER('autopaysuccess'), visit__visit_id, NULL)))
  AS set_up_auto_payments,
SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('selectAction')
    AND LOWER(state__view__current_page__elements__standardized_name) IN( LOWER('callUsButton'), LOWER('contactUs')) , visit__visit_id, NULL)))
  AS call_support_or_request_callback,
SUM(IF(((LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('modalView')
    AND LOWER(state__view__modal__name) = LOWER('internetresetsuccess-modal')
    AND LOWER(state__view__previous_page__page_name)
    IN (LOWER('internetTab'),LOWER('equipmentHome'))
    AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab'))
    OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('modalView')
    AND LOWER(state__view__modal__name) = LOWER('resetsuccess-modal')
    AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab'))
    OR (LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('modalView')
    AND LOWER(state__view__modal__name) = LOWER('resetsuccess-modal')
    AND LOWER(state__view__previous_page__page_name) IN (LOWER('internetTab'),LOWER('equipmentHome'))
    AND LOWER(state__view__current_page__page_name) <> LOWER('voiceTab'))), 1, 0))
      +
      SUM(IF(LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
      AND LOWER(message__name) = LOWER('modalView')
      AND LOWER(state__view__modal__name) = LOWER('resetsuccess-modal')
      AND LOWER(state__view__current_page__page_name) = LOWER('voiceTab'), 1, 0))
  AS modem_router_resets,
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('selectAction')
    AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')
    AND LOWER(state__view__current_page__elements__standardized_name)
      IN(LOWER('troubleshoot'),LOWER('internetConnectIssues'),LOWER('internetTroubleshoot')), 1, 0))
  AS refresh_digital_receiver_requests,
SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('modalView')
    AND LOWER(state__view__modal__name) = LOWER('cancelappointment-modal') , visit__visit_id, NULL)))
  AS canceled_appointments,
SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('apiCall')
    AND LOWER(application__api__path) = LOWER('/care/api/v1/appointment/reschedule')
    AND application__api__response_code RLIKE '2.*' , visit__visit_id, NULL)))
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
  AND LOWER(visit__account__details__mso) IN (LOWER('BH'),LOWER('CHARTER'),LOWER('TWC'))
GROUP BY
fiscal_month,
CASE
    WHEN LOWER(visit__account__details__mso) = LOWER('BH') THEN '${hiveconf:bhn}'
    WHEN LOWER(visit__account__details__mso) = LOWER('CHARTER') THEN '${hiveconf:chtr}'
    WHEN LOWER(visit__account__details__mso) = LOWER('TWC') THEN '${hiveconf:twc}'
    ELSE visit__account__details__mso
  END
;

SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** No MSO ***** ------------------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_app_quantum
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

            'password_reset_success',password_reset_success,
            'id_recovery_success',id_recovery_success

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_app_quantum1
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
----------------------------- ***** With MSO ***** -----------------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_app_quantum
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

            'unique_visitors',unique_visitors,
            'web_sessions_visits',web_sessions_visits,
            'support_section_page_views',support_section_page_views,
            'view_statements',view_statements,
            'one_time_payments',one_time_payments,
            'set_up_auto_payments',set_up_auto_payments,
            'call_support_or_request_callback',call_support_or_request_callback,
            'modem_router_resets',modem_router_resets,
            'refresh_digital_receiver_requests',refresh_digital_receiver_requests,
            'canceled_appointments',canceled_appointments,
            'rescheduled_appointments',rescheduled_appointments

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_app_quantum2
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
--------------------- ***** END App Quantum Metrics ***** ---------------------
--------------------------------------------------------------------------------

';
