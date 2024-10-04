set hive.vectorized.execution.enabled = false;
set hive.auto.convert.join=false;
set tez.am.resource.memory.mb=12288;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

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
SET company=All_Companies;
SET domain=app;
SET source_table1=asp_v_venona_events_portals_msa;
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

TRUNCATE TABLE asp_metric_pivot_app_quantum_daily PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm});
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_app_quantum_daily PURGE;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** Quantum Metrics ***** ---------------------
--------------------------------------------------------------------------------
';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_app_quantum_daily AS
SELECT
'${hiveconf:company}' as company,
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = 'myspectrum'
    AND message__name = 'loginStop'
    AND operation__success = TRUE, visit__visit_id, Null)))
  AS site_unique_auth,
SUM(IF(LOWER(visit__application_details__application_name) = 'myspectrum'
    AND message__name = 'pageView'
    AND state__view__current_page__app_section = 'support' , 1, 0))
  AS support_page_views,
SUM(IF(LOWER(visit__application_details__application_name) = 'myspectrum'
    AND message__name = 'selectAction'
    AND state__view__current_page__elements__standardized_name = 'callUsButton' , 1, 0))
  AS call_support,
SUM(IF(LOWER(visit__application_details__application_name) = 'myspectrum'
    AND message__name = 'pageView'
    AND state__view__current_page__page_name = 'statementDetail' , 1, 0))
  AS view_statement,
SUM(IF(LOWER(visit__application_details__application_name) = 'myspectrum'
    AND message__name = 'pageView'
    and state__view__current_page__page_name IN ('paySuccess','paySuccessAutoPay')
    and state__view__previous_page__page_name ='makePayment' , 1, 0))
  AS one_time_payment_flow_successes_all,-- diff def than existing ETL
SUM(IF(LOWER(visit__application_details__application_name) = 'myspectrum'
    AND message__name = 'pageView'
    AND state__view__current_page__page_name = 'paySuccessAutoPay'
    and state__view__previous_page__page_name ='makePayment', 1, 0))
  AS one_time_payment_flow_successes_with_ap_enroll, -- new metric
SUM(IF(LOWER(visit__application_details__application_name) = 'myspectrum'
    AND message__name = 'pageView'
    AND state__view__current_page__page_name = 'paySuccess'
    and state__view__previous_page__page_name ='makePayment' , 1, 0))
  AS one_time_payment_flow_successes_without_ap_enroll, -- new metric
SUM(IF(LOWER(visit__application_details__application_name) = 'myspectrum'
    AND message__name = 'selectAction'
    AND state__view__current_page__page_name = 'tvTab'
    AND state__view__current_page__elements__standardized_name IN('troubleshoot','internetConnectIssues','internetTroubleshoot') , 1, 0)) AS tv_equipment_reset_flow_starts,
SUM(IF(LOWER(visit__application_details__application_name) = 'myspectrum'
    AND message__name = 'modalView'
    AND state__view__modal__name = 'cancelappointment-modal' , 1, 0))
  AS cancelled_appointments,
SUM(IF(LOWER(visit__application_details__application_name) = 'myspectrum'
    AND message__name = 'selectAction'
    AND state__view__current_page__elements__standardized_name = 'reschedApptConfirm' , 1, 0))
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
'${hiveconf:company}'
;

SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** Quantum Metrics ***** ---------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_app_quantum_daily
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
            'site_unique_auth',site_unique_auth,
            'support_page_views',support_page_views,
            'call_support',call_support,
            'view_statement',view_statement,
            'one_time_payment_flow_successes_all',one_time_payment_flow_successes_all,
            'one_time_payment_flow_successes_with_ap_enroll',one_time_payment_flow_successes_with_ap_enroll,
            'one_time_payment_flow_successes_without_ap_enroll',one_time_payment_flow_successes_without_ap_enroll,
            'tv_equipment_reset_flow_starts',tv_equipment_reset_flow_starts,
            'cancelled_appointments',cancelled_appointments,
            'rescheduled_appointments',rescheduled_appointments
          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_app_quantum_daily
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;
SELECT'
--------------------------------------------------------------------------------
--------------------- ***** END App Quantum Metrics ***** ---------------------
--------------------------------------------------------------------------------

';
