USE ${env:ENVIRONMENT};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.tez.container.size=16000;

set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_venona_msa_onboarding_metrics_preliminary;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_venona_msa_onboarding_metrics_preliminary
(
  mso STRING,
  device_type STRING,
  tutorial_completed STRING,
  aggregation STRING,
  metric_name STRING,
  metric_value DOUBLE,
  process_date_time_denver STRING,
  process_identity STRING,
  label_date_denver STRING,
  grain STRING)
;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- Visit Metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
INSERT INTO TABLE ${env:TMP_db}.asp_venona_msa_onboarding_metrics_preliminary
SELECT
        mso,
        device_type,
        tutorial_completed,
        'visits' as aggregation,
        metric_name,
        metric_value,
        '${env:ProcessTimestamp}' as process_date_time_denver,
        '${env:ProcessUser}' AS process_identity,
        '${env:label_date_denver}' AS label_date_denver,
        '${env:grain}' AS grain
  FROM
        (
        SELECT
                mso,
                device_type,
                tutorial_completed,
                MAP(
                  'portals_tutorial_views', SUM(portals_tutorial_views),
                  'portals_tutorial_start', SUM(portals_tutorial_start),
                  'portals_tutorial_otp', SUM(portals_tutorial_otp),
                  'portals_tutorial_equip', SUM(portals_tutorial_equip),
                  'portals_tutorial_acct', SUM(portals_tutorial_acct),
                  'portals_tutorial_support', SUM(portals_tutorial_support),

                  'portals_tutorial_end_start', SUM(portals_tutorial_end_start),
                  'portals_tutorial_end_otp', SUM(portals_tutorial_end_otp),
                  'portals_tutorial_end_equip', SUM(portals_tutorial_end_equip),
                  'portals_tutorial_end_acct', SUM(portals_tutorial_end_acct),

                  'portals_otp_starts_both', SUM(portals_otp_starts_both),
                  'portals_otp_failures_both', SUM(portals_otp_failures_both),
                  'portals_otp_successes_both', SUM(portals_otp_successes_both),
                  'portals_atp_starts_both', SUM(portals_atp_starts_both),
                  'portals_atp_failures_both', SUM(portals_atp_failures_both),
                  'portals_atp_successes_both', SUM(portals_atp_successes_both),
                  'portals_equip_starts_both', SUM(portals_equip_starts_both),
                  'portals_equip_failures_both', SUM(portals_equip_failures_both),
                  'portals_equip_successes_both', SUM(portals_equip_successes_both),
                  'portals_acct_both', SUM(portals_acct_both),
                  'portals_support_both', SUM(portals_support_both)
                ) AS tmp_map
          FROM
                  (
                  SELECT
                          visit_id,
                          mso,
                          device_type,
                          IF(SUM(portals_tutorial_support) > 0, 'yes', 'no') AS tutorial_completed,
                          IF(SUM(portals_tutorial_views) > 0, 1, 0) AS portals_tutorial_views,
                          IF(SUM(portals_tutorial_start) > 0, 1, 0) AS portals_tutorial_start,
                          IF(SUM(portals_tutorial_otp) > 0, 1, 0) AS portals_tutorial_otp,
                          IF(SUM(portals_tutorial_equip) > 0, 1, 0) AS portals_tutorial_equip,
                          IF(SUM(portals_tutorial_acct) > 0, 1, 0) AS portals_tutorial_acct,
                          IF(SUM(portals_tutorial_support) > 0, 1, 0) AS portals_tutorial_support,

                          IF(SUM(portals_tutorial_cancel_start) > 0, 1, 0) AS portals_tutorial_cancel_start,
                          IF(SUM(portals_tutorial_cancel_otp) > 0, 1, 0) AS portals_tutorial_cancel_otp,
                          IF(SUM(portals_tutorial_cancel_equip) > 0, 1, 0) AS portals_tutorial_cancel_equip,
                          IF(SUM(portals_tutorial_cancel_acct) > 0, 1, 0) AS portals_tutorial_cancel_acct,

                          IF(SUM(portals_tutorial_start) > 0 and SUM(portals_tutorial_otp) =0,1,0) AS portals_tutorial_end_start,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_tutorial_equip) =0,1,0) AS portals_tutorial_end_otp,
                          IF(SUM(portals_tutorial_equip) > 0 and SUM(portals_tutorial_acct) =0,1,0) AS portals_tutorial_end_equip,
                          IF(SUM(portals_tutorial_acct) > 0 and SUM(portals_tutorial_support) =0,1,0) AS portals_tutorial_end_acct,

                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_one_time_payment_starts) > 0,1,0) AS portals_otp_starts_both,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_one_time_payment_failures) > 0,1,0) AS portals_otp_failures_both,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_one_time_payment_successes) > 0,1,0) AS portals_otp_successes_both,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_set_up_auto_payment_starts) > 0,1,0) AS portals_atp_starts_both,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_set_up_auto_payment_failures) > 0,1,0) AS portals_atp_failures_both,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_set_up_auto_payment_successes) > 0,1,0) AS portals_atp_successes_both,
                          IF(SUM(portals_tutorial_equip) > 0 and SUM(portals_all_equipment_reset_flow_starts) > 0,1,0) AS portals_equip_starts_both,
                          IF(SUM(portals_tutorial_equip) > 0 and SUM(portals_all_equipment_reset_flow_failures) > 0,1,0) AS portals_equip_failures_both,
                          IF(SUM(portals_tutorial_equip) > 0 and SUM(portals_all_equipment_reset_flow_successes) > 0,1,0) AS portals_equip_successes_both,
                          IF(SUM(portals_tutorial_acct) > 0 and SUM(portals_account_settings_your_info_tab_views) > 0,1,0) AS portals_acct_both,
                          IF(SUM(portals_tutorial_support) > 0 and SUM(portals_support_article_select) > 0,1,0) AS portals_support_both,

                          IF(SUM(portals_one_time_payment_starts) > 0, 1, 0) AS portals_one_time_payment_starts,
                          IF(SUM(portals_one_time_payment_failures) > 0, 1, 0) AS portals_one_time_payment_failures,
                          IF(SUM(portals_one_time_payment_successes) > 0, 1, 0) AS portals_one_time_payment_successes,
                          IF(SUM(portals_set_up_auto_payment_starts) > 0, 1, 0) AS portals_set_up_auto_payment_starts,
                          IF(SUM(portals_set_up_auto_payment_failures) > 0, 1, 0) AS portals_set_up_auto_payment_failures,
                          IF(SUM(portals_set_up_auto_payment_successes) > 0, 1, 0) AS portals_set_up_auto_payment_successes,
                          IF(SUM(portals_all_equipment_reset_flow_starts) > 0, 1, 0) AS portals_all_equipment_reset_flow_starts,
                          IF(SUM(portals_all_equipment_reset_flow_failures) > 0, 1, 0) AS portals_all_equipment_reset_flow_failures,
                          IF(SUM(portals_all_equipment_reset_flow_successes) > 0, 1, 0) AS portals_all_equipment_reset_flow_successes,
                          IF(SUM(portals_account_settings_your_info_tab_views) > 0, 1, 0) AS portals_account_settings_your_info_tab_views,
                          IF(SUM(portals_support_article_select) > 0, 1, 0) AS portals_support_article_select
                  FROM
                          (
                          SELECT
                                 mso,
                                 device_type,
                                 visit_id,

                                 SUM(portals_tutorial_views) AS portals_tutorial_views,
                                 SUM(portals_tutorial_start_page_one_page_view) AS portals_tutorial_start,
                                 SUM(portals_tutorial_otp_page_two_page_view) AS portals_tutorial_otp,
                                 SUM(portals_tutorial_equip_page_three_page_view) AS portals_tutorial_equip,
                                 SUM(portals_tutorial_account_page_four_page_view) AS portals_tutorial_acct,
                                 SUM(portals_tutorial_account_page_five_page_view) AS portals_tutorial_support,

                                 SUM(portals_cancel_tutorial_start_page_one) AS portals_tutorial_cancel_start,
                                 SUM(portals_cancel_tutorial_otp_page_two) AS portals_tutorial_cancel_otp,
                                 SUM(portals_cancel_tutorial_equip_page_three) AS portals_tutorial_cancel_equip,
                                 SUM(portals_cancel_tutorial_account_page_four) AS portals_tutorial_cancel_acct,

                                 SUM(portals_one_time_payment_starts) AS portals_one_time_payment_starts,
                                 SUM(portals_one_time_payment_failures) AS portals_one_time_payment_failures,
                                 SUM(portals_one_time_payment_successes) AS portals_one_time_payment_successes,
                                 SUM(portals_set_up_auto_payment_starts) AS portals_set_up_auto_payment_starts,
                                 SUM(portals_set_up_auto_payment_failures) AS portals_set_up_auto_payment_failures,
                                 SUM(portals_set_up_auto_payment_successes) AS portals_set_up_auto_payment_successes,
                                 SUM(portals_all_equipment_reset_flow_starts) AS portals_all_equipment_reset_flow_starts,
                                 SUM(portals_all_equipment_reset_flow_failures) AS portals_all_equipment_reset_flow_failures,
                                 SUM(portals_all_equipment_reset_flow_successes) AS portals_all_equipment_reset_flow_successes,
                                 SUM(portals_account_settings_your_info_tab_views) as portals_account_settings_your_info_tab_views,
                                 SUM(portals_support_article_select) as portals_support_article_select
                            FROM asp_v_venona_metric_agg_portals
                           WHERE (denver_date >= '${env:START_DATE}' AND denver_date < '${env:END_DATE}')
                             AND application_name ='myspectrum'
                           GROUP BY
                                 mso,
                                 device_type,
                                 visit_id
                          ) sumfirst
                  GROUP BY
                        visit_id,
                        mso,
                        device_type
                  ) sets
         GROUP BY
               mso,
               device_type,
               tutorial_completed
        ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- Household Metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
INSERT INTO TABLE ${env:TMP_db}.asp_venona_msa_onboarding_metrics_preliminary

SELECT
        mso,
        device_type,
        tutorial_completed,
        'households' as aggregation,
        metric_name,
        metric_value,
        '${env:ProcessTimestamp}' as process_date_time_denver,
        '${env:ProcessUser}' AS process_identity,
        '${env:label_date_denver}' AS label_date_denver,
        '${env:grain}' AS grain
  FROM
        (
        SELECT
                mso,
                device_type,
                tutorial_completed,
                MAP(
                  'portals_tutorial_views', SUM(portals_tutorial_views),
                  'portals_tutorial_start', SUM(portals_tutorial_start),
                  'portals_tutorial_otp', SUM(portals_tutorial_otp),
                  'portals_tutorial_equip', SUM(portals_tutorial_equip),
                  'portals_tutorial_acct', SUM(portals_tutorial_acct),
                  'portals_tutorial_support', SUM(portals_tutorial_support),

                  'portals_tutorial_end_start', SUM(portals_tutorial_end_start),
                  'portals_tutorial_end_otp', SUM(portals_tutorial_end_otp),
                  'portals_tutorial_end_equip', SUM(portals_tutorial_end_equip),
                  'portals_tutorial_end_acct', SUM(portals_tutorial_end_acct),

                  'portals_otp_starts_both', SUM(portals_otp_starts_both),
                  'portals_otp_failures_both', SUM(portals_otp_failures_both),
                  'portals_otp_successes_both', SUM(portals_otp_successes_both),
                  'portals_atp_starts_both', SUM(portals_atp_starts_both),
                  'portals_atp_failures_both', SUM(portals_atp_failures_both),
                  'portals_atp_successes_both', SUM(portals_atp_successes_both),
                  'portals_equip_starts_both', SUM(portals_equip_starts_both),
                  'portals_equip_failures_both', SUM(portals_equip_failures_both),
                  'portals_equip_successes_both', SUM(portals_equip_successes_both),
                  'portals_acct_both', SUM(portals_acct_both),
                  'portals_support_both', SUM(portals_support_both)
                ) AS tmp_map
          FROM
                  (
                  SELECT
                          portals_unique_acct_key,
                          mso,
                          device_type,
                          IF(SUM(portals_tutorial_support) > 0, 'yes', 'no') AS tutorial_completed,
                          IF(SUM(portals_tutorial_views) > 0, 1, 0) AS portals_tutorial_views,
                          IF(SUM(portals_tutorial_start) > 0, 1, 0) AS portals_tutorial_start,
                          IF(SUM(portals_tutorial_otp) > 0, 1, 0) AS portals_tutorial_otp,
                          IF(SUM(portals_tutorial_equip) > 0, 1, 0) AS portals_tutorial_equip,
                          IF(SUM(portals_tutorial_acct) > 0, 1, 0) AS portals_tutorial_acct,
                          IF(SUM(portals_tutorial_support) > 0, 1, 0) AS portals_tutorial_support,

                          IF(SUM(portals_tutorial_cancel_start) > 0, 1, 0) AS portals_tutorial_cancel_start,
                          IF(SUM(portals_tutorial_cancel_otp) > 0, 1, 0) AS portals_tutorial_cancel_otp,
                          IF(SUM(portals_tutorial_cancel_equip) > 0, 1, 0) AS portals_tutorial_cancel_equip,
                          IF(SUM(portals_tutorial_cancel_acct) > 0, 1, 0) AS portals_tutorial_cancel_acct,

                          IF(SUM(portals_tutorial_start) > 0 and SUM(portals_tutorial_otp) =0,1,0) AS portals_tutorial_end_start,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_tutorial_equip) =0,1,0) AS portals_tutorial_end_otp,
                          IF(SUM(portals_tutorial_equip) > 0 and SUM(portals_tutorial_acct) =0,1,0) AS portals_tutorial_end_equip,
                          IF(SUM(portals_tutorial_acct) > 0 and SUM(portals_tutorial_support) =0,1,0) AS portals_tutorial_end_acct,

                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_one_time_payment_starts) > 0,1,0) AS portals_otp_starts_both,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_one_time_payment_failures) > 0,1,0) AS portals_otp_failures_both,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_one_time_payment_successes) > 0,1,0) AS portals_otp_successes_both,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_set_up_auto_payment_starts) > 0,1,0) AS portals_atp_starts_both,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_set_up_auto_payment_failures) > 0,1,0) AS portals_atp_failures_both,
                          IF(SUM(portals_tutorial_otp) > 0 and SUM(portals_set_up_auto_payment_successes) > 0,1,0) AS portals_atp_successes_both,
                          IF(SUM(portals_tutorial_equip) > 0 and SUM(portals_all_equipment_reset_flow_starts) > 0,1,0) AS portals_equip_starts_both,
                          IF(SUM(portals_tutorial_equip) > 0 and SUM(portals_all_equipment_reset_flow_failures) > 0,1,0) AS portals_equip_failures_both,
                          IF(SUM(portals_tutorial_equip) > 0 and SUM(portals_all_equipment_reset_flow_successes) > 0,1,0) AS portals_equip_successes_both,
                          IF(SUM(portals_tutorial_acct) > 0 and SUM(portals_account_settings_your_info_tab_views) > 0,1,0) AS portals_acct_both,
                          IF(SUM(portals_tutorial_support) > 0 and SUM(portals_support_article_select) > 0,1,0) AS portals_support_both,

                          IF(SUM(portals_one_time_payment_starts) > 0, 1, 0) AS portals_one_time_payment_starts,
                          IF(SUM(portals_one_time_payment_failures) > 0, 1, 0) AS portals_one_time_payment_failures,
                          IF(SUM(portals_one_time_payment_successes) > 0, 1, 0) AS portals_one_time_payment_successes,
                          IF(SUM(portals_set_up_auto_payment_starts) > 0, 1, 0) AS portals_set_up_auto_payment_starts,
                          IF(SUM(portals_set_up_auto_payment_failures) > 0, 1, 0) AS portals_set_up_auto_payment_failures,
                          IF(SUM(portals_set_up_auto_payment_successes) > 0, 1, 0) AS portals_set_up_auto_payment_successes,
                          IF(SUM(portals_all_equipment_reset_flow_starts) > 0, 1, 0) AS portals_all_equipment_reset_flow_starts,
                          IF(SUM(portals_all_equipment_reset_flow_failures) > 0, 1, 0) AS portals_all_equipment_reset_flow_failures,
                          IF(SUM(portals_all_equipment_reset_flow_successes) > 0, 1, 0) AS portals_all_equipment_reset_flow_successes,
                          IF(SUM(portals_account_settings_your_info_tab_views) > 0, 1, 0) AS portals_account_settings_your_info_tab_views,
                          IF(SUM(portals_support_article_select) > 0, 1, 0) AS portals_support_article_select
                  FROM
                          (
                          SELECT
                                 mso,
                                 device_type,
                                 portals_unique_acct_key,

                                 SUM(portals_tutorial_views) AS portals_tutorial_views,
                                 SUM(portals_tutorial_start_page_one_page_view) AS portals_tutorial_start,
                                 SUM(portals_tutorial_otp_page_two_page_view) AS portals_tutorial_otp,
                                 SUM(portals_tutorial_equip_page_three_page_view) AS portals_tutorial_equip,
                                 SUM(portals_tutorial_account_page_four_page_view) AS portals_tutorial_acct,
                                 SUM(portals_tutorial_account_page_five_page_view) AS portals_tutorial_support,
                                 SUM(portals_cancel_tutorial_start_page_one) AS portals_tutorial_cancel_start,
                                 SUM(portals_cancel_tutorial_otp_page_two) AS portals_tutorial_cancel_otp,
                                 SUM(portals_cancel_tutorial_equip_page_three) AS portals_tutorial_cancel_equip,
                                 SUM(portals_cancel_tutorial_account_page_four) AS portals_tutorial_cancel_acct,

                                 SUM(portals_one_time_payment_starts) AS portals_one_time_payment_starts,
                                 SUM(portals_one_time_payment_failures) AS portals_one_time_payment_failures,
                                 SUM(portals_one_time_payment_successes) AS portals_one_time_payment_successes,
                                 SUM(portals_set_up_auto_payment_starts) AS portals_set_up_auto_payment_starts,
                                 SUM(portals_set_up_auto_payment_failures) AS portals_set_up_auto_payment_failures,
                                 SUM(portals_set_up_auto_payment_successes) AS portals_set_up_auto_payment_successes,
                                 SUM(portals_all_equipment_reset_flow_starts) AS portals_all_equipment_reset_flow_starts,
                                 SUM(portals_all_equipment_reset_flow_failures) AS portals_all_equipment_reset_flow_failures,
                                 SUM(portals_all_equipment_reset_flow_successes) AS portals_all_equipment_reset_flow_successes,
                                 SUM(portals_account_settings_your_info_tab_views) as portals_account_settings_your_info_tab_views,
                                 SUM(portals_support_article_select) as portals_support_article_select
                            FROM asp_v_venona_metric_agg_portals
                           WHERE (denver_date >= '${env:START_DATE}' AND denver_date < '${env:END_DATE}')
                             AND application_name ='myspectrum'
                           GROUP BY
                                 mso,
                                 device_type,
                                 portals_unique_acct_key
                          ) sumfirst
                  GROUP BY
                        portals_unique_acct_key,
                        mso,
                        device_type
                  ) sets
         GROUP BY
               mso,
               device_type,
               tutorial_completed
        ) mapit
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- Insert Overwrite into paritioned table
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

INSERT OVERWRITE TABLE asp_msa_onboarding_metrics PARTITION (label_date_denver, grain)

SELECT  mso,
        device_type,
        tutorial_completed,
        aggregation,
        metric_name,
        metric_value,
        process_date_time_denver,
        process_identity,
        label_date_denver,
        grain
FROM ${env:TMP_db}.asp_venona_msa_onboarding_metrics_preliminary
;
