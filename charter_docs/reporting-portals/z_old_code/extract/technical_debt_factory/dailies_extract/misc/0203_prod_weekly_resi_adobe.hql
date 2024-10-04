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
-------------------------- ***** Table Cleanup ***** ---------------------------
--------------------------------------------------------------------------------

';

TRUNCATE TABLE asp_metric_pivot_resi_adobe PARTITION(platform,domain,company,data_source,year_fiscal_month);
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_twc PURGE;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** TWC ***** ---------------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_resi_adobe_twc AS
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
fiscal_month AS year_fiscal_month
FROM prod.${hiveconf:source_table4} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_utc} >= ADD_MONTHS('${hiveconf:END_DATE_utc}${hiveconf:dt_hr}',-6)
  AND ${hiveconf:alias}.${hiveconf:partition_dt_utc} <  '${hiveconf:END_DATE_utc}${hiveconf:dt_hr}')
GROUP BY
fiscal_month,
'${hiveconf:twc}'
;


SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
------------------------------- ***** TWC ***** --------------------------------
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
          FROM ${env:TMP_db}.asp_metric_pull_resi_adobe_twc
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** END Resi Adobe Metrics ***** ----------------------
--------------------------------------------------------------------------------

';
