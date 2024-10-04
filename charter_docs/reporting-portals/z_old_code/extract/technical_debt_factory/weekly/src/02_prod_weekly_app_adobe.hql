set hive.vectorized.execution.enabled = false;
set tez.am.resource.memory.mb=12288;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
---------------------------- ***** APP ADOBE ***** -----------------------------
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
SET mytwc=MyTWC;
SET msa=My Spectrum;
SET domain=app;
SET source_table1=asp_v_twc_app_events;
SET source_table2=asp_v_app_figures_downloads;
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

TRUNCATE TABLE asp_metric_pivot_app_adobe_wk PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm});
DROP TABLE IF EXISTS ${env:TMP_db}.asp_metric_pull_app_adobe_mytwc_wk PURGE;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 1: Wide Data Pull ***** ----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** MyTWC ***** -------------------------------
--------------------------------------------------------------------------------

';

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.asp_metric_pull_app_adobe_mytwc_wk AS
SELECT
  '${hiveconf:mytwc}' as company,

-- begin definitions

SIZE(COLLECT_SET(visit__device__enc_uuid))
  AS unique_visitors,
SIZE(COLLECT_SET(visit__visit_id))
  AS web_sessions_visits,
SIZE(COLLECT_LIST(CASE WHEN (message__name RLIKE ('MyTWC > Help.*')
    AND message__category = 'Page View' )THEN visit__visit_id ELSE NULL END))
  AS support_section_page_views,
SIZE(COLLECT_LIST(CASE WHEN message__name
    in ('AMACTION:MyTWC > Billing > Make Payment > Statement Download','AMACTION:MyTWC > Billing > Statement History > Statement Download')
    THEN visit__visit_id ELSE NULL END))
  AS view_statements,
SIZE(COLLECT_SET(CASE WHEN message__name in ('MyTWC > Billing > Make Payment > Successful Payment') THEN visit__visit_id ELSE NULL END))AS one_time_payment,
SIZE(COLLECT_SET(CASE
    WHEN message__name in ( 'AutoPay > Setup > New Credit Card or Debit Card > Success',
      'AutoPay > Setup > Saved Card > Success',
      'AutoPay > Setup > Bank Account > Success',
      'AutoPay > Setup > Saved EFT > Success',
      'AutoPay > Setup > Debit Card > Success',
      'AutoPay > Setup > Credit Card > Success')
      THEN visit__visit_id
      ELSE NULL
      END))
  AS set_up_auto_payments,
SIZE(COLLECT_SET(CASE
    WHEN (message__name RLIKE ('MyTWC > Services & Troubleshoot > Equipment > Internet > Troubleshooting.*Connection')
    OR message__name RLIKE ('MyTWC > Services & Troubleshoot > Equipment > Home Phone > Troubleshooting  ?(No Dial|Trouble making|Poor Call)')
    )
    THEN visit__visit_id
    ELSE NULL
    END))
  AS modem_router_resets,
SIZE(COLLECT_SET(CASE
    WHEN message__name RLIKE ('MyTWC > Services & Troubleshoot > Equipment > TV > Troubleshooting.*> Su')
    THEN visit__visit_id
    ELSE NULL
    END))
  AS refresh_digital_receiver_requests,
SIZE(COLLECT_SET(CASE
    WHEN message__name in ('MyTWC > Appointment Manager > Canceled')
    THEN visit__visit_id
    ELSE NULL
    END))
  AS canceled_appointments,
SIZE(COLLECT_SET(CASE
    WHEN message__name in ('MyTWC > Appointment Manager > Reschedule Complete')
    THEN visit__visit_id
    ELSE NULL
    END))
  AS rescheduled_appointments,
SIZE(COLLECT_LIST(CASE
    WHEN message__name RLIKE '.*Contact(Now|Later)Success.*'
    THEN visit__visit_id
    ELSE NULL
    END))
  AS call_support_or_request_callback,

-- end definitions

  'asp' AS platform,
  '${hiveconf:domain}' AS domain,
  '${hiveconf:source}' AS data_source,
  '${env:PART_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table1} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
  ON epoch_converter(cast(${hiveconf:ts}*${hiveconf:ts_multiplier} AS BIGINT),'America/Denver') = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_utc} >= '${env:START_DATE_TZ}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_utc} < '${env:END_DATE_TZ}')
GROUP BY
'${env:PART_DATE}',
'${hiveconf:mytwc}'
;

SELECT'
--------------------------------------------------------------------------------
------------------- ***** STEP 2: Pivot to long/skinny ***** -------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** MyTWC ***** -------------------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_app_adobe_wk
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

            'unique_visitors',unique_visitors,
            'web_sessions_visits',web_sessions_visits,
            'support_section_page_views',support_section_page_views,
            'view_statements',view_statements,
            'set_up_auto_payments',set_up_auto_payments,
            'modem_router_resets',modem_router_resets,
            'refresh_digital_receiver_requests',refresh_digital_receiver_requests,
            'canceled_appointments',canceled_appointments,
            'rescheduled_appointments',rescheduled_appointments,
            'call_support_or_request_callback',call_support_or_request_callback

          ) as map_column
          FROM ${env:TMP_db}.asp_metric_pull_app_adobe_mytwc_wk
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, metric_value
;

SELECT'
--------------------------------------------------------------------------------
------------------ ***** STEP 3: One-Off Metric Inserts ***** ------------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE asp_metric_pivot_app_adobe_wk
PARTITION(platform,domain,company,data_source,${hiveconf:partition_nm})
SELECT

SUM(daily_downloads) AS metric_value,
'app_downloads' AS metric,
'asp' AS platform,
'${hiveconf:domain}' AS domain,
CASE WHEN product_name = 'My BHN' THEN '${hiveconf:bhn}'
     WHEN product_name = 'My TWC®' THEN '${hiveconf:mytwc}'
     WHEN product_name = 'My Spectrum' THEN '${hiveconf:msa}'
     ELSE 'UNDEFINED'
  END as company,
'App Figures' AS data_source,
'${env:PART_DATE}' AS ${hiveconf:partition_nm}
FROM prod.${hiveconf:source_table2} ${hiveconf:alias}
LEFT JOIN prod_lkp.chtr_fiscal_month fm
    ON ${hiveconf:alias}.${hiveconf:partition_dt_fid} = fm.partition_date
  WHERE (${hiveconf:alias}.${hiveconf:partition_dt_fid} >= '${env:START_DATE}'
  AND ${hiveconf:alias}.${hiveconf:partition_dt_fid} <  '${env:END_DATE}')
  AND product_id IN ('40423315838','40425298890','11340139','15570967','40423315838.0','40425298890.0')
  AND daily_downloads IS NOT NULL
GROUP BY
'${env:PART_DATE}',
CASE WHEN product_name = 'My BHN' THEN '${hiveconf:bhn}'
     WHEN product_name = 'My TWC®' THEN '${hiveconf:mytwc}'
     WHEN product_name = 'My Spectrum' THEN '${hiveconf:msa}'
     ELSE 'UNDEFINED'
  END
;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** END App Adobe Metrics ***** -----------------------
--------------------------------------------------------------------------------

';
