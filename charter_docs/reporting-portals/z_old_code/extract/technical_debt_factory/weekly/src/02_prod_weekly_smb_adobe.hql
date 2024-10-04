set hive.vectorized.execution.enabled = false;
set tez.am.resource.memory.mb=12288;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
--------------------------- ***** SMB ADOBE ***** ------------------------------
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
SET domain=smb;
SET source_table1=asp_v_bhn_my_services_events;
SET source_table2=asp_v_bhn_bill_pay_events;
SET source_table3=asp_v_twc_bus_global_events;
SET source_table4=;
SET source_table_fid=asp_v_federated_identity;
SET pivot=asp_metric_pivot_smb_adobe_wk;
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

TRUNCATE TABLE ${hiveconf:pivot} PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm});
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_smb_adobe_bhn1_wk PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_smb_adobe_bhn2_wk PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_smb_adobe_twc_wk PURGE;SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** BHN 1 ***** -------------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_smb_adobe_bhn1_wk AS
SELECT
  '${hiveconf:bhn}' as company,

-- begin definitions

SIZE(COLLECT_SET(visit__visit_id))
  AS web_sessions_visits,
SUM(IF(message__category = 'Page View'
    AND visit__settings['post_prop9'] RLIKE '.*small-medium.*'
    AND concat_ws(',',(message__name)) RLIKE ".*medium:support.*", 1, 0))
  AS support_section_page_views,
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
    AND ARRAY_CONTAINS(message__name, 'Registration Success'), visit__visit_id, NULL)))
  AS ids_created,
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
    AND ARRAY_CONTAINS(message__name, 'Registration Submit') ,visit__visit_id, NULL)))
  AS id_creation_attempts,
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
    AND ARRAY_CONTAINS(message__name, 'Recover Username Success') ,visit__visit_id, NULL)))
  AS id_recovery_success,
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
    AND ARRAY_CONTAINS(message__name, 'Recover Username Step1') ,visit__visit_id, NULL)))
  AS id_recovery_attempts,
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
    AND ARRAY_CONTAINS(message__name, 'Reset Password Success') ,visit__visit_id, NULL)))
  AS password_reset_success,
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
    AND ARRAY_CONTAINS(message__name, 'Reset Password Step1') ,visit__visit_id, NULL)))
  AS password_reset_attempts,

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
'${hiveconf:bhn}'
;

SELECT'
--------------------------------------------------------------------------------
----------------------------- ***** BHN 2 ***** --------------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_smb_adobe_bhn2_wk AS
SELECT
  '${hiveconf:bhn}' as company,

-- begin definitions

SUM(IF(message__name RLIKE('.*stmtdownload/.*')
    AND message__category = 'Exit Link'
    AND visit__settings["post_evar9"] = 'SMB', 1, 0))
  AS view_statements,
SUM(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 31')
    AND state__view__current_page__page_type='SMB',1,0))
  AS one_time_payments,
SUM(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 24')
    AND state__view__current_page__page_type='SMB',1,0))
  AS set_up_auto_payments,

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

SELECT'
--------------------------------------------------------------------------------
----------------------------- ***** TWC  ***** --------------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_smb_adobe_twc_wk AS
SELECT
  '${hiveconf:twc}' as company,

-- begin definitions

SIZE(COLLECT_SET(visit__visit_id))
  AS web_sessions_visits,
SUM(IF(((state__view__current_page__page_name NOT RLIKE '.*channelpartners.*|.*channel partners.*|.*enterprise.*')
    AND state__view__current_page__page_name RLIKE '.*support.*'
    AND message__category = 'Page View' )
    OR (message__triggered_by <> 'enterprise'
      AND message__triggered_by <> 'channel partners'
      AND state__view__current_page__page_name RLIKE '.*faq.*'
      AND message__category = 'Page View'),1,0))
  AS support_section_page_views,
SUM(IF(visit__device__device_type RLIKE '.*220.*'
    AND state__view__current_page__elements__name = 'my account > billing > statements: statement download', 1, 0 ))
  AS view_statements,
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name RLIKE '.*step 4.*'
    AND (state__view__current_page__elements__name RLIKE '.*fdp.*|.*one time.*') ,visit__visit_id, NULL)))
  AS one_time_payments,
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name RLIKE '.*step 4.*'
    AND (state__view__current_page__elements__name RLIKE '.*recurring.*') ,visit__visit_id, NULL)))
  AS set_up_auto_payments,
SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*253.*', visit__visit_id, NULL)))
  AS ids_created,
SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*249.*', visit__visit_id, NULL)))
  AS id_creation_attempts,
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > users > add user save', visit__visit_id, NULL)))
  AS new_sub_users_created,
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name =  'my account > users > add user', visit__visit_id, NULL)))
  AS new_sub_user_creation_attempts,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username> email sent', visit__visit_id, NULL)))
  AS id_recovery_success,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username > step 1', visit__visit_id, NULL)))
  AS id_recovery_attempts,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot password > change password confirmation', visit__visit_id, NULL)))
  AS password_reset_success,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name RLIKE ('bc > forgot password > step ?1'), visit__visit_id, NULL)))
  AS password_reset_attempts,
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > support > cancel appointment submit' ,visit__visit_id, NULL)))
  AS canceled_appointments,
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > support > reschedule appointment submit' ,visit__visit_id, NULL)))
  AS rescheduled_appointments,

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
'${hiveconf:twc}'
;

SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
------------------------------- ***** BHN 1 ***** ------------------------------
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
            'ids_created',ids_created,
            'id_creation_attempts',id_creation_attempts,
            'id_recovery_success',id_recovery_success,
            'id_recovery_attempts',id_recovery_attempts,
            'password_reset_success',password_reset_success,
            'password_reset_attempts',password_reset_attempts

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_smb_adobe_bhn1_wk
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** BHN 2 ***** -------------------------------
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

            'view_statements',view_statements,
            'one_time_payments',one_time_payments,
            'set_up_auto_payments',set_up_auto_payments

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_smb_adobe_bhn2_wk
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
------------------------------- ***** TWC ***** --------------------------------
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
            'ids_created',ids_created,
            'id_creation_attempts',id_creation_attempts,
            'new_sub_users_created',new_sub_users_created,
            'new_sub_user_creation_attempts',new_sub_user_creation_attempts,
            'id_recovery_success',id_recovery_success,
            'id_recovery_attempts',id_recovery_attempts,
            'password_reset_success',password_reset_success,
            'password_reset_attempts',password_reset_attempts,
            'canceled_appointments',canceled_appointments,
            'rescheduled_appointments',rescheduled_appointments

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_smb_adobe_twc_wk
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
------------------ ***** STEP 3: One-Off Metric Inserts ***** ------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE ${hiveconf:pivot}
PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm})
SELECT
SIZE(COLLECT_SET(IF(account_number_aes256 IS NOT NULL,account_number_aes256, NULL))) AS metric_value,
'hhs_logged_in' AS metric,
'asp' AS platform,
'${hiveconf:domain}' AS domain,
CASE WHEN footprint = 'Charter' THEN 'CHTR' ELSE footprint END as company,
'${hiveconf:source}' AS data_source,
'${env:PART_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table_fid} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
    ON ${hiveconf:alias}.${hiveconf:partition_dt_fid} = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_fid} >= '${env:START_DATE}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_fid} <  '${env:END_DATE}')
  AND LOWER(source_app) IN('portals-idp-comm')
  AND account_number_aes256 IS NOT NULL
  AND is_success = true
GROUP BY
'${env:PART_DATE}',
CASE WHEN footprint = 'Charter' THEN 'CHTR' ELSE footprint END
;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** END SMB Adobe Metrics ***** ----------------------
--------------------------------------------------------------------------------

';
