USE ${env:TMP_db};

CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable_daily} (run_date string);
CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable_fiscal_monthly} (run_date string);
CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable_monthly} (run_date string);
CREATE TABLE IF NOT EXISTS ${env:ReprocessDateTable_weekly} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_daily} VALUES('${env:RUN_DATE}');
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_fiscal_monthly} VALUES('${env:RUN_DATE}');
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_monthly} VALUES('${env:RUN_DATE}');
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_weekly} VALUES('${env:RUN_DATE}');


USE ${env:ENVIRONMENT};

--DROP TABLE asp_billing_set_agg PURGE;

CREATE TABLE IF NOT EXISTS asp_billing_set_agg
(
  application_name STRING,
  mso STRING,
  autopay_status STRING,
  billing_status STRING,
  metric_name STRING,
  metric_value DOUBLE,
  grouping_id INT,
  process_date_time_denver STRING,
  process_identity STRING)
PARTITIONED BY (label_date_denver STRING, grain STRING)
;
--- View that feeds Billing Detail Tableau Dashboard ---

DROP VIEW IF EXISTS asp_v_billing_detail;

CREATE VIEW IF NOT EXISTS asp_v_billing_detail   AS SELECT

  CASE WHEN grain = 'daily'                  THEN 'Daily'
       WHEN grain = 'weekly'                 THEN 'Weekly'
       WHEN grain = 'fiscal_monthly'         THEN 'Fiscal Monthly'
       WHEN grain = 'monthly'                THEN 'Monthly'
       ELSE grain END                        AS `Cadence`,

  CASE WHEN mso IN ('"TWC"','L-TWC')         THEN 'TWC'
       WHEN mso IN ('"BHN"','L-BHN')         THEN 'BHN'
       WHEN mso IN ('"CHTR"','L-CHTR')       THEN 'CHTR'
       When mso IN ('unknown','MSO-MISSING') THEN 'Unknown'
       ELSE mso END                          AS `Company`,

  application_name                           AS `Portal`,
  autopay_status                             AS `AutoPay Status`,
  billing_status                             AS `Billing Status`,
  grouping_id                                AS `Grouping ID`,
  metric_name                                AS `Metric Name`,
  metric_value                               AS `Metric Value`,
  label_date_denver                          AS `Denver Date`,

  CASE
       WHEN metric_name RLIKE 'portals_account_billing_support_views_.*' THEN 'Account & Billing Support Section Views'
       WHEN metric_name RLIKE 'portals_autopay_enroll_radio_toggle_.*' THEN 'Auto-Pay Enroll Radio Toggle'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_exit_.*' THEN 'Billing Delivery Preference: Exit'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_keep_paperless_.*' THEN 'Billing Delivery Preference: Keep Paperless'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_manage_paperless_.*' THEN 'Billing Delivery Preference: Manage Paperless'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_paper_success_.*' THEN 'Billing Delivery Preference: Paper Success'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_paper_failure_.*' THEN 'Billing Delivery Preference: Paper Failure'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_paper_.*' THEN 'Billing Delivery Preference: Keep Paper'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_paperless_enroll_from_homepage_.*' THEN 'Billing Delivery Preference: Paperless Enroll from Homepage'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_paperless_failure_.*' THEN 'Billing Delivery Preference: Paperless Failure'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_paperless_submit_from_billinghome_.*' THEN 'Billing Delivery Preference: Paperless Submit from Billing'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_paperless_submit_from_homepage_.*' THEN 'Billing Delivery Preference: Paperless Submit from Homepage'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_paperless_submit_from_paperless_.*' THEN 'Billing Delivery Preference: Paperless Un-Enroll from Paperless'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_paperless_submit_.*' THEN 'Billing Delivery Preference: Paperless Submit'
       WHEN metric_name RLIKE 'portals_bill_delivery_pref_paperless_success_.*' THEN 'Billing Delivery Preference: Paperless Success'
       WHEN metric_name RLIKE 'portals_bill_pay_clicks_from_local_nav_.*' THEN 'Bill Pay Clicks From Local Nav'
       WHEN metric_name RLIKE 'portals_billing_feature_interaction_.*' THEN 'Billing Feature Interaction'
       WHEN metric_name RLIKE 'portals_one_time_payment_checking_account_.*' THEN  'Customers with Final Payment Method = Checking'
       WHEN metric_name RLIKE 'portals_one_time_payment_checking_.*' THEN  'One Time Payment Method Continue - Checking'
       WHEN metric_name RLIKE 'portals_one_time_payment_credit_debit_card_.*' THEN 'Customers with Final Payment Method = Card'
       WHEN metric_name RLIKE 'portals_one_time_payment_credit_.*' THEN 'One Time Payment Method Continue - Credit'
       WHEN metric_name RLIKE 'portals_one_time_payment_failures_.*' THEN 'One Time Payment Flow: Failures'
       WHEN metric_name RLIKE 'portals_one_time_payment_savings_account_.*' THEN 'Customers with Final Payment Method = Savings'
       WHEN metric_name RLIKE 'portals_one_time_payment_savings_.*' THEN 'One Time Payment Method Continue - Savings'

       WHEN metric_name RLIKE 'portals_one_time_payment_select_amount_.*' THEN 'One Time Payment Flow: Select Amount'
       WHEN metric_name RLIKE 'portals_one_time_payment_select_date_.*' THEN 'One Time Payment Flow: Select Date'
       WHEN metric_name RLIKE 'portals_one_time_payment_select_payment_method_.*' THEN 'One Time Payment Flow: Select Payment Method'
       WHEN metric_name RLIKE 'portals_one_time_payment_starts_.*' THEN 'One Time Payment Flow: Starts'
       WHEN metric_name RLIKE 'portals_one_time_payment_submission_new_payment_method_.*' THEN  'One Time Payment Submission: New Payment Method'
       WHEN metric_name RLIKE 'portals_one_time_payment_submission_new_payment_method_not_stored_.*' THEN  'One Time Payment Submission: New Payment Method (Not Stored)'
       WHEN metric_name RLIKE 'portals_one_time_payment_submission_new_payment_method_stored_.*' THEN  'One Time Payment Submission: New Method (Stored)'
       WHEN metric_name RLIKE 'portals_one_time_payment_submission_previously_stored_payment_method_.*' THEN  'One Time Payment Submission: Previously Stored Payment Method'
       WHEN metric_name RLIKE 'portals_one_time_payment_submits_.*' THEN 'One Time Payment Flow: Submits'
       WHEN metric_name RLIKE 'portals_one_time_payment_successes_with_ap_enroll_.*' THEN 'One Time Payment Flow: Successes With Auto-Pay Enrollment'
       WHEN metric_name RLIKE 'portals_one_time_payment_successes_without_ap_enroll_.*' THEN 'One Time Payment Flow: Successes Without Auto-Pay Enrollment'
       WHEN metric_name RLIKE 'portals_one_time_payment_successes_.*' THEN 'One Time Payment Flow: Successes All'

       WHEN metric_name RLIKE 'portals_set_up_auto_payment_failures_.*' THEN 'Auto-Pay Flow: Enrollment Failures'
       WHEN metric_name RLIKE 'portals_set_up_auto_payment_starts_.*' THEN 'Auto-Pay Flow: Enrollment Starts'
       WHEN metric_name RLIKE 'portals_set_up_auto_payment_submits_plus_otp_.*' THEN 'Auto-Pay Flow: Enrollment Submits with One Time Payment'
       WHEN metric_name RLIKE 'portals_set_up_auto_payment_submits_.*' THEN 'Auto-Pay Flow: Enrollment Submits'
       WHEN metric_name RLIKE 'portals_set_up_auto_payment_successes_with_payment_.*' THEN 'Auto-Pay Flow: Enrollment Successes With Payment'
       WHEN metric_name RLIKE 'portals_set_up_auto_payment_successes_without_payment_.*' THEN 'Auto-Pay Flow: Enrollment Successes Without Payment'
       WHEN metric_name RLIKE 'portals_set_up_auto_payment_successes_.*' THEN 'Auto-Pay Flow: Enrollment Successes All'
       WHEN metric_name RLIKE 'portals_view_online_statments_.*' THEN 'View Online Statement'
       ELSE 'Not used' END                   AS `Display Metric Name`,

  CASE
       WHEN metric_name RLIKE '.*_instances' THEN 'Instances'
       WHEN metric_name RLIKE '.*_devices'   THEN 'Devices'
       WHEN metric_name RLIKE '.*_visits'    THEN 'Visits '
       WHEN metric_name RLIKE '.*_hh'        THEN 'Households'
       ELSE 'Unknown' END                    AS `Aggregation`

  FROM asp_billing_set_agg
 WHERE ((grain                      IN ('fiscal_monthly','monthly'))
        OR (grain = 'weekly'        AND date_format(label_date_denver, 'u') = 4)
        OR (grain = 'daily'         AND label_date_denver >= (DATE_SUB(CURRENT_DATE, 62)) ))
   AND application_name             IN ('smb','specnet')
;
