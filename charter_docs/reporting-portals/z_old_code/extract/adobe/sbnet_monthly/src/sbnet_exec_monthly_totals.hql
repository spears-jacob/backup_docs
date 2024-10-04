USE ${env:ENVIRONMENT};

-- Truncate previous month's data from tmp table
TRUNCATE TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_totals_instances;
TRUNCATE TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_totals_visits;

INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_totals_instances
SELECT
  src.year_month as year_month,
  SUM(src.hhs_logged_in) as hhs_logged_in,
  SUM(src.total_hhs) as total_hhs,
  SUM(src.hhs_logged_in)/SUM(src.total_hhs) as percent_hhs_logged_in,
  (SUM(src.hhs_logged_in) - lag_months_grouped.hhs_logged_in_1_mo_lag)/(lag_months_grouped.hhs_logged_in_1_mo_lag) as unique_hhs_mom_change,
  (SUM(src.hhs_logged_in) - lag_months_grouped.hhs_logged_in_3_mo_lag)/(lag_months_grouped.hhs_logged_in_3_mo_lag) as unique_hhs_3month_change,
  SUM(src.my_account_page_views) as my_account_page_views,
  SUM(src.total_login_attempts) as total_login_attempts,
  SUM(src.total_login_successes) as total_login_successes,
  SUM(src.total_login_successes)/SUM(src.total_login_attempts) as percent_login_success,
  SUM(src.rescheduled_service_appointments) as rescheduled_service_appointments,
  SUM(src.cancelled_service_appointments) as cancelled_service_appointments,
  SUM(src.support_page_views) as support_page_views,
  SUM(src.online_statement_views) as online_statement_views,
  SUM(src.one_time_payment_attempts) as one_time_payment_attempts,
  SUM(src.one_time_payments) as one_time_payments,
  SUM(IF(src.company IN ('L-BHN','L-CHTR'),src.one_time_payments,0))/SUM(IF(src.company IN ('L-BHN','L-CHTR'),src.one_time_payment_attempts,0)) as percent_one_time_payment_success,
  SUM(src.auto_pay_setup_attempts) as auto_pay_setup_attempts,
  SUM(src.auto_pay_setup_successes) as auto_pay_setup_successes,
  SUM(IF(src.company IN ('L-BHN','L-CHTR'),src.auto_pay_setup_successes,0))/SUM(IF(src.company IN ('L-BHN','L-CHTR'),src.auto_pay_setup_attempts,0)) as percent_auto_pay_success,
  SUM(src.total_account_creation_attempts) as total_account_creation_attempts,
  SUM(src.total_new_accounts_created) as total_new_accounts_created,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_new_accounts_created,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_account_creation_attempts,0)) as percent_total_account_creation_success,
  SUM(src.new_account_creation_attempts_on_chtr_network) as new_account_creation_attempts_on_chtr_network,
  SUM(src.new_accounts_created_on_chtr_network) as new_accounts_created_on_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.new_accounts_created_on_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.new_account_creation_attempts_on_chtr_network,0)) as percent_account_creation_success_on_chtr_network,
  SUM(src.new_account_creation_attempts_off_chtr_network) as new_account_creation_attempts_off_chtr_network,
  SUM(src.new_accounts_created_off_chtr_network) as new_accounts_created_off_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.new_accounts_created_off_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.new_account_creation_attempts_off_chtr_network,0)) as percent_account_creation_success_off_chtr_network,
  SUM(src.total_sub_user_creation_attempts) as total_sub_user_creation_attempts,
  SUM(src.total_new_sub_users_created) as total_new_sub_users_created,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_new_sub_users_created,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_sub_user_creation_attempts,0)) as percent_total_sub_user_creation_success,
  SUM(src.sub_user_creation_attempts_on_chtr_network) as sub_user_creation_attempts_on_chtr_network,
  SUM(src.sub_users_created_on_chtr_network) as sub_users_created_on_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.sub_users_created_on_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.sub_user_creation_attempts_on_chtr_network,0)) as percent_sub_user_creation_success_on_chtr_network,
  SUM(src.sub_user_creation_attempts_off_chtr_network) as sub_user_creation_attempts_off_chtr_network,
  SUM(src.sub_users_created_off_chtr_network) as sub_users_created_off_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.sub_users_created_off_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.sub_user_creation_attempts_off_chtr_network,0)) as percent_sub_user_creation_success_off_chtr_network,
  SUM(src.total_username_recovery_attempts) as total_username_recovery_attempts,
  SUM(src.total_username_recovery_successes) as total_username_recovery_successes,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),total_username_recovery_successes,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_username_recovery_attempts,0)) as percent_total_username_recovery_success,
  SUM(src.username_recovery_attempts_on_chtr_network) as username_recovery_attempts_on_chtr_network,
  SUM(src.username_recovery_successes_on_chtr_network) as username_recovery_successes_on_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.username_recovery_successes_on_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.username_recovery_attempts_on_chtr_network,0)) as percent_username_recovery_success_on_chtr_network,
  SUM(src.username_recovery_attempts_off_chtr_network) as username_recovery_attempts_off_chtr_network,
  SUM(src.username_recovery_successes_off_chtr_network) as username_recovery_successes_off_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.username_recovery_successes_off_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.username_recovery_attempts_off_chtr_network,0)) as percent_username_recovery_success_off_chtr_network,
  SUM(src.total_attempts_to_reset_password) as total_attempts_to_reset_password,
  SUM(src.total_successful_password_resets) as total_successful_password_resets,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),total_successful_password_resets,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_attempts_to_reset_password,0)) as percent_total_password_reset_success,
  SUM(src.attempts_to_reset_password_on_chtr_network) as attempts_to_reset_password_on_chtr_network,
  SUM(src.successful_password_resets_on_chtr_network) as successful_password_resets_on_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.successful_password_resets_on_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.attempts_to_reset_password_on_chtr_network,0)) as percent_password_reset_success_on_chtr_network,
  SUM(src.attempts_to_reset_password_off_chtr_network) as attempts_to_reset_password_off_chtr_network,
  SUM(src.successful_password_resets_off_chtr_network) as successful_password_resets_off_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.successful_password_resets_off_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.attempts_to_reset_password_off_chtr_network,0)) as percent_password_reset_success_off_chtr_network
FROM sbnet_exec_monthly_agg_instances src
LEFT JOIN
  (SELECT
    MAX(lag_months.hhs_logged_in_1_mo_lag) as hhs_logged_in_1_mo_lag,
    MAX(lag_months.hhs_logged_in_3_mo_lag) as hhs_logged_in_3_mo_lag
  FROM(
    SELECT
    IF(year_month=
      CASE
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '01' THEN CONCAT(CAST((CAST(SUBSTR('${hiveconf:YEAR_MONTH}',0,4) as Int)-1) as String),'-12')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '02' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'01')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '03' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'02')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '04' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'03')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '05' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'04')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '06' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'05')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '07' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'06')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '08' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'07')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '09' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'08')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '10' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'09')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '11' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'10')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '12' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'11')
      END,
      hhs_logged_in,0) as hhs_logged_in_1_mo_lag,
    IF(year_month=
      CASE
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '01' THEN CONCAT(CAST((CAST(SUBSTR('${hiveconf:YEAR_MONTH}',0,4) as Int)-1) as String),'-10')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '02' THEN CONCAT(CAST((CAST(SUBSTR('${hiveconf:YEAR_MONTH}',0,4) as Int)-1) as String),'-11')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '03' THEN CONCAT(CAST((CAST(SUBSTR('${hiveconf:YEAR_MONTH}',0,4) as Int)-1) as String),'-12')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '04' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'01')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '05' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'02')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '06' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'03')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '07' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'04')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '08' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'05')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '09' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'06')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '10' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'07')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '11' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'08')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '12' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'09')
      END,
      hhs_logged_in,0) as hhs_logged_in_3_mo_lag
  FROM sbnet_exec_monthly_agg_instances
  WHERE company in ('Total Combined')
    ) lag_months
  ) lag_months_grouped
WHERE src.year_month='${hiveconf:YEAR_MONTH}'
GROUP BY
  src.year_month,
  lag_months_grouped.hhs_logged_in_1_mo_lag,
  lag_months_grouped.hhs_logged_in_3_mo_lag
;

INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_totals_visits
SELECT
  src.year_month as year_month,
  SUM(src.hhs_logged_in) as hhs_logged_in,
  SUM(src.total_hhs) as total_hhs,
  SUM(src.hhs_logged_in)/SUM(src.total_hhs) as percent_hhs_logged_in,
  (SUM(src.hhs_logged_in) - lag_months_grouped.hhs_logged_in_1_mo_lag)/(lag_months_grouped.hhs_logged_in_1_mo_lag) as unique_hhs_mom_change,
  (SUM(src.hhs_logged_in) - lag_months_grouped.hhs_logged_in_3_mo_lag)/(lag_months_grouped.hhs_logged_in_3_mo_lag) as unique_hhs_3month_change,
  SUM(src.my_account_page_views) as my_account_page_views,
  SUM(src.total_login_attempts) as total_login_attempts,
  SUM(src.total_login_successes) as total_login_successes,
  SUM(src.total_login_successes)/SUM(src.total_login_attempts) as percent_login_success,
  SUM(src.rescheduled_service_appointments) as rescheduled_service_appointments,
  SUM(src.cancelled_service_appointments) as cancelled_service_appointments,
  SUM(src.support_page_views) as support_page_views,
  SUM(src.online_statement_views) as online_statement_views,
  SUM(src.one_time_payment_attempts) as one_time_payment_attempts,
  SUM(src.one_time_payments) as one_time_payments,
  SUM(src.one_time_payments_with_autopay_setup) AS one_time_payments_with_autopay_setup, -- 2017-12-28
  SUM(IF(src.company IN ('L-BHN','L-CHTR'),src.one_time_payments,0))/SUM(IF(src.company IN ('L-BHN','L-CHTR'),src.one_time_payment_attempts,0)) as percent_one_time_payment_success,
  SUM(src.auto_pay_setup_attempts) as auto_pay_setup_attempts,
  SUM(src.auto_pay_setup_successes) as auto_pay_setup_successes,
  SUM(IF(src.company IN ('L-BHN','L-CHTR'),src.auto_pay_setup_successes,0))/SUM(IF(src.company IN ('L-BHN','L-CHTR'),src.auto_pay_setup_attempts,0)) as percent_auto_pay_success,
  SUM(src.total_account_creation_attempts) as total_account_creation_attempts,
  SUM(src.total_new_accounts_created) as total_new_accounts_created,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_new_accounts_created,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_account_creation_attempts,0)) as percent_total_account_creation_success,
  SUM(src.new_account_creation_attempts_on_chtr_network) as new_account_creation_attempts_on_chtr_network,
  SUM(src.new_accounts_created_on_chtr_network) as new_accounts_created_on_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.new_accounts_created_on_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.new_account_creation_attempts_on_chtr_network,0)) as percent_account_creation_success_on_chtr_network,
  SUM(src.new_account_creation_attempts_off_chtr_network) as new_account_creation_attempts_off_chtr_network,
  SUM(src.new_accounts_created_off_chtr_network) as new_accounts_created_off_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.new_accounts_created_off_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.new_account_creation_attempts_off_chtr_network,0)) as percent_account_creation_success_off_chtr_network,
  SUM(src.total_sub_user_creation_attempts) as total_sub_user_creation_attempts,
  SUM(src.total_new_sub_users_created) as total_new_sub_users_created,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_new_sub_users_created,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_sub_user_creation_attempts,0)) as percent_total_sub_user_creation_success,
  SUM(src.sub_user_creation_attempts_on_chtr_network) as sub_user_creation_attempts_on_chtr_network,
  SUM(src.sub_users_created_on_chtr_network) as sub_users_created_on_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.sub_users_created_on_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.sub_user_creation_attempts_on_chtr_network,0)) as percent_sub_user_creation_success_on_chtr_network,
  SUM(src.sub_user_creation_attempts_off_chtr_network) as sub_user_creation_attempts_off_chtr_network,
  SUM(src.sub_users_created_off_chtr_network) as sub_users_created_off_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.sub_users_created_off_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.sub_user_creation_attempts_off_chtr_network,0)) as percent_sub_user_creation_success_off_chtr_network,
  SUM(src.total_username_recovery_attempts) as total_username_recovery_attempts,
  SUM(src.total_username_recovery_successes) as total_username_recovery_successes,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),total_username_recovery_successes,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_username_recovery_attempts,0)) as percent_total_username_recovery_success,
  SUM(src.username_recovery_attempts_on_chtr_network) as username_recovery_attempts_on_chtr_network,
  SUM(src.username_recovery_successes_on_chtr_network) as username_recovery_successes_on_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.username_recovery_successes_on_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.username_recovery_attempts_on_chtr_network,0)) as percent_username_recovery_success_on_chtr_network,
  SUM(src.username_recovery_attempts_off_chtr_network) as username_recovery_attempts_off_chtr_network,
  SUM(src.username_recovery_successes_off_chtr_network) as username_recovery_successes_off_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.username_recovery_successes_off_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.username_recovery_attempts_off_chtr_network,0)) as percent_username_recovery_success_off_chtr_network,
  SUM(src.total_attempts_to_reset_password) as total_attempts_to_reset_password,
  SUM(src.total_successful_password_resets) as total_successful_password_resets,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),total_successful_password_resets,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.total_attempts_to_reset_password,0)) as percent_total_password_reset_success,
  SUM(src.attempts_to_reset_password_on_chtr_network) as attempts_to_reset_password_on_chtr_network,
  SUM(src.successful_password_resets_on_chtr_network) as successful_password_resets_on_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.successful_password_resets_on_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.attempts_to_reset_password_on_chtr_network,0)) as percent_password_reset_success_on_chtr_network,
  SUM(src.attempts_to_reset_password_off_chtr_network) as attempts_to_reset_password_off_chtr_network,
  SUM(src.successful_password_resets_off_chtr_network) as successful_password_resets_off_chtr_network,
  SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.successful_password_resets_off_chtr_network,0))/SUM(IF(src.company IN ('L-TWC','L-CHTR'),src.attempts_to_reset_password_off_chtr_network,0)) as percent_password_reset_success_off_chtr_network
FROM sbnet_exec_monthly_agg_visits src
LEFT JOIN
  (SELECT
    MAX(lag_months.hhs_logged_in_1_mo_lag) as hhs_logged_in_1_mo_lag,
    MAX(lag_months.hhs_logged_in_3_mo_lag) as hhs_logged_in_3_mo_lag
  FROM(
    SELECT
    IF(year_month=
      CASE
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '01' THEN CONCAT(CAST((CAST(SUBSTR('${hiveconf:YEAR_MONTH}',0,4) as Int)-1) as String),'-12')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '02' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'01')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '03' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'02')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '04' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'03')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '05' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'04')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '06' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'05')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '07' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'06')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '08' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'07')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '09' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'08')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '10' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'09')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '11' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'10')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '12' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'11')
      END,
      hhs_logged_in,0) as hhs_logged_in_1_mo_lag,
    IF(year_month=
      CASE
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '01' THEN CONCAT(CAST((CAST(SUBSTR('${hiveconf:YEAR_MONTH}',0,4) as Int)-1) as String),'-10')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '02' THEN CONCAT(CAST((CAST(SUBSTR('${hiveconf:YEAR_MONTH}',0,4) as Int)-1) as String),'-11')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '03' THEN CONCAT(CAST((CAST(SUBSTR('${hiveconf:YEAR_MONTH}',0,4) as Int)-1) as String),'-12')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '04' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'01')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '05' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'02')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '06' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'03')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '07' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'04')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '08' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'05')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '09' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'06')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '10' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'07')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '11' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'08')
        WHEN SPLIT('${hiveconf:YEAR_MONTH}','-')[1] = '12' THEN CONCAT(SUBSTR('${hiveconf:YEAR_MONTH}',0,5),'09')
      END,
      hhs_logged_in,0) as hhs_logged_in_3_mo_lag
  FROM sbnet_exec_monthly_agg_visits
  WHERE company in ('Total Combined')
    ) lag_months
  ) lag_months_grouped
WHERE src.year_month='${hiveconf:YEAR_MONTH}'
GROUP BY
  src.year_month,
  lag_months_grouped.hhs_logged_in_1_mo_lag,
  lag_months_grouped.hhs_logged_in_3_mo_lag
;
