USE ${env:ENVIRONMENT};

INSERT INTO TABLE sbnet_exec_monthly_agg_instances PARTITION (year_month='${hiveconf:YEAR_MONTH}')
SELECT
  src.company as company,
  MAX(fid.unique_users_logged_in) as hhs_logged_in,
  MAX(src.total_hhs) as total_hhs,
  MAX(fid.unique_users_logged_in)/MAX(src.total_hhs) as percent_hhs_logged_in,
  (MAX(fid.unique_users_logged_in) - lag_months_grouped.hhs_logged_in_1_mo_lag)/(lag_months_grouped.hhs_logged_in_1_mo_lag) as unique_hhs_mom_change,
  (MAX(fid.unique_users_logged_in) - lag_months_grouped.hhs_logged_in_3_mo_lag)/(lag_months_grouped.hhs_logged_in_3_mo_lag) as unique_hhs_3month_change,
  MAX(src.my_account_page_views) as my_account_page_views,
  MAX(IF(fid.total_login_attempts IS NOT NULL,fid.total_login_attempts,0)) as total_login_attempts,
  MAX(IF(fid.total_login_successes IS NOT NULL,fid.total_login_successes,0)) as total_login_successes,
  CAST(NULL AS INT) as percent_login_success,
  MAX(src.rescheduled_service_appointments) as rescheduled_service_appointments,
  MAX(src.cancelled_service_appointments) as cancelled_service_appointments,
  MAX(src.support_page_views) as support_page_views,
  MAX(src.online_statement_views) as online_statement_views,
  MAX(src.one_time_payment_attempts) as one_time_payment_attempts,
  MAX(src.one_time_payments) as one_time_payments,
  MAX(src.one_time_payments)/MAX(src.one_time_payment_attempts) as percent_one_time_payment_success,
  MAX(src.auto_pay_setup_attempts) as auto_pay_setup_attempts,
  MAX(src.auto_pay_setup_successes) as auto_pay_setup_successes,
  MAX(src.auto_pay_setup_successes)/MAX(src.auto_pay_setup_attempts) as percent_auto_pay_success,
  MAX(src.total_account_creation_attempts) as total_account_creation_attempts,
  MAX(src.total_new_accounts_created) as total_new_accounts_created,
  MAX(src.total_new_accounts_created)/MAX(src.total_account_creation_attempts) as percent_total_account_creation_success,
  MAX(src.new_account_creation_attempts_on_chtr_network) as new_account_creation_attempts_on_chtr_network,
  MAX(src.new_accounts_created_on_chtr_network) as new_accounts_created_on_chtr_network,
  MAX(src.new_accounts_created_on_chtr_network)/MAX(src.new_account_creation_attempts_on_chtr_network) as percent_account_creation_success_on_chtr_network,
  MAX(src.new_account_creation_attempts_off_chtr_network) as new_account_creation_attempts_off_chtr_network,
  MAX(src.new_accounts_created_off_chtr_network) as new_accounts_created_off_chtr_network,
  MAX(src.new_accounts_created_off_chtr_network)/MAX(src.new_account_creation_attempts_off_chtr_network) as percent_account_creation_success_off_chtr_network,
  MAX(src.total_sub_user_creation_attempts) as total_sub_user_creation_attempts,
  MAX(src.total_new_sub_users_created) as total_new_sub_users_created,
  MAX(src.total_new_sub_users_created)/MAX(src.total_sub_user_creation_attempts) as percent_total_sub_user_creation_success,
  MAX(src.sub_user_creation_attempts_on_chtr_network) as sub_user_creation_attempts_on_chtr_network,
  MAX(src.sub_users_created_on_chtr_network) as sub_users_created_on_chtr_network,
  MAX(src.sub_users_created_on_chtr_network)/MAX(src.sub_user_creation_attempts_on_chtr_network) as percent_sub_user_creation_success_on_chtr_network,
  MAX(src.sub_user_creation_attempts_off_chtr_network) as sub_user_creation_attempts_off_chtr_network,
  MAX(src.sub_users_created_off_chtr_network) as sub_users_created_off_chtr_network,
  MAX(src.sub_users_created_off_chtr_network)/MAX(src.sub_user_creation_attempts_off_chtr_network) as percent_sub_user_creation_success_off_chtr_network,
  MAX(src.total_username_recovery_attempts) as total_username_recovery_attempts,
  MAX(src.total_username_recovery_successes) as total_username_recovery_successes,
  MAX(src.total_username_recovery_successes)/MAX(src.total_username_recovery_attempts) as percent_total_username_recovery_success,
  MAX(src.username_recovery_attempts_on_chtr_network) as username_recovery_attempts_on_chtr_network,
  MAX(src.username_recovery_successes_on_chtr_network) as username_recovery_successes_on_chtr_network,
  MAX(src.username_recovery_successes_on_chtr_network)/MAX(src.username_recovery_attempts_on_chtr_network) as percent_username_recovery_success_on_chtr_network,
  MAX(src.username_recovery_attempts_off_chtr_network) as username_recovery_attempts_off_chtr_network,
  MAX(src.username_recovery_successes_off_chtr_network) as username_recovery_successes_off_chtr_network,
  MAX(src.username_recovery_successes_off_chtr_network)/MAX(src.username_recovery_attempts_off_chtr_network) as percent_username_recovery_success_off_chtr_network,
  MAX(src.total_attempts_to_reset_password) as total_attempts_to_reset_password,
  MAX(src.total_successful_password_resets) as total_successful_password_resets,
  MAX(src.total_successful_password_resets)/MAX(src.total_attempts_to_reset_password) as percent_total_password_reset_success,
  MAX(src.attempts_to_reset_password_on_chtr_network) as attempts_to_reset_password_on_chtr_network,
  MAX(src.successful_password_resets_on_chtr_network) as successful_password_resets_on_chtr_network,
  MAX(src.successful_password_resets_on_chtr_network)/MAX(src.attempts_to_reset_password_on_chtr_network) as percent_password_reset_success_on_chtr_network,
  MAX(src.attempts_to_reset_password_off_chtr_network) as attempts_to_reset_password_off_chtr_network,
  MAX(src.successful_password_resets_off_chtr_network) as successful_password_resets_off_chtr_network,
  MAX(src.successful_password_resets_off_chtr_network)/MAX(src.attempts_to_reset_password_off_chtr_network) as percent_password_reset_success_off_chtr_network
FROM ${env:TMP_db}.sbnet_exec_monthly_agg_src_instances src
LEFT JOIN
  (SELECT
    lag_months.company,
    MAX(lag_months.hhs_logged_in_1_mo_lag) as hhs_logged_in_1_mo_lag,
    MAX(lag_months.hhs_logged_in_3_mo_lag) as hhs_logged_in_3_mo_lag
  FROM(
    SELECT
    company,
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
  WHERE company in ('L-CHTR','L-TWC','L-BHN')
    ) lag_months
  GROUP BY
    lag_months.company
  ) lag_months_grouped
ON src.company = lag_months_grouped.company
LEFT JOIN ${env:TMP_db}.sbnet_fed_ID_auths_monthly fid
ON src.company = fid.company
GROUP BY
  src.company,
  lag_months_grouped.hhs_logged_in_1_mo_lag,
  lag_months_grouped.hhs_logged_in_3_mo_lag

;

INSERT INTO TABLE sbnet_exec_monthly_agg_visits PARTITION (year_month='${hiveconf:YEAR_MONTH}')
SELECT
  src.company as company,
  MAX(fid.unique_users_logged_in) as hhs_logged_in,
  MAX(src.total_hhs) as total_hhs,
  MAX(fid.unique_users_logged_in)/MAX(src.total_hhs) as percent_hhs_logged_in,
  (MAX(fid.unique_users_logged_in) - lag_months_grouped.hhs_logged_in_1_mo_lag)/(lag_months_grouped.hhs_logged_in_1_mo_lag) as unique_hhs_mom_change,
  (MAX(fid.unique_users_logged_in) - lag_months_grouped.hhs_logged_in_3_mo_lag)/(lag_months_grouped.hhs_logged_in_3_mo_lag) as unique_hhs_3month_change,
  MAX(src.my_account_page_views) as my_account_page_views,
  MAX(IF(fid.total_login_attempts IS NOT NULL,fid.total_login_attempts,0)) as total_login_attempts,
  MAX(IF(fid.total_login_successes IS NOT NULL,fid.total_login_successes,0)) as total_login_successes,
  CAST(NULL AS INT) as percent_login_success,
  MAX(src.rescheduled_service_appointments) as rescheduled_service_appointments,
  MAX(src.cancelled_service_appointments) as cancelled_service_appointments,
  MAX(src.support_page_views) as support_page_views,
  MAX(src.online_statement_views) as online_statement_views,
  MAX(src.one_time_payment_attempts) as one_time_payment_attempts,
  MAX(src.one_time_payments) as one_time_payments,
  MAX(src.one_time_payments_with_autopay_setup) AS one_time_payments_with_autopay_setup, -- 2017-12-28 added
  MAX(src.one_time_payments)/MAX(src.one_time_payment_attempts) as percent_one_time_payment_success,
  MAX(src.auto_pay_setup_attempts) as auto_pay_setup_attempts,
  MAX(src.auto_pay_setup_successes) as auto_pay_setup_successes,
  MAX(src.auto_pay_setup_successes)/MAX(src.auto_pay_setup_attempts) as percent_auto_pay_success,
  MAX(src.total_account_creation_attempts) as total_account_creation_attempts,
  MAX(src.total_new_accounts_created) as total_new_accounts_created,
  MAX(src.total_new_accounts_created)/MAX(src.total_account_creation_attempts) as percent_total_account_creation_success,
  MAX(src.new_account_creation_attempts_on_chtr_network) as new_account_creation_attempts_on_chtr_network,
  MAX(src.new_accounts_created_on_chtr_network) as new_accounts_created_on_chtr_network,
  MAX(src.new_accounts_created_on_chtr_network)/MAX(src.new_account_creation_attempts_on_chtr_network) as percent_account_creation_success_on_chtr_network,
  MAX(src.new_account_creation_attempts_off_chtr_network) as new_account_creation_attempts_off_chtr_network,
  MAX(src.new_accounts_created_off_chtr_network) as new_accounts_created_off_chtr_network,
  MAX(src.new_accounts_created_off_chtr_network)/MAX(src.new_account_creation_attempts_off_chtr_network) as percent_account_creation_success_off_chtr_network,
  MAX(src.total_sub_user_creation_attempts) as total_sub_user_creation_attempts,
  MAX(src.total_new_sub_users_created) as total_new_sub_users_created,
  MAX(src.total_new_sub_users_created)/MAX(src.total_sub_user_creation_attempts) as percent_total_sub_user_creation_success,
  MAX(src.sub_user_creation_attempts_on_chtr_network) as sub_user_creation_attempts_on_chtr_network,
  MAX(src.sub_users_created_on_chtr_network) as sub_users_created_on_chtr_network,
  MAX(src.sub_users_created_on_chtr_network)/MAX(src.sub_user_creation_attempts_on_chtr_network) as percent_sub_user_creation_success_on_chtr_network,
  MAX(src.sub_user_creation_attempts_off_chtr_network) as sub_user_creation_attempts_off_chtr_network,
  MAX(src.sub_users_created_off_chtr_network) as sub_users_created_off_chtr_network,
  MAX(src.sub_users_created_off_chtr_network)/MAX(src.sub_user_creation_attempts_off_chtr_network) as percent_sub_user_creation_success_off_chtr_network,
  MAX(src.total_username_recovery_attempts) as total_username_recovery_attempts,
  MAX(src.total_username_recovery_successes) as total_username_recovery_successes,
  MAX(src.total_username_recovery_successes)/MAX(src.total_username_recovery_attempts) as percent_total_username_recovery_success,
  MAX(src.username_recovery_attempts_on_chtr_network) as username_recovery_attempts_on_chtr_network,
  MAX(src.username_recovery_successes_on_chtr_network) as username_recovery_successes_on_chtr_network,
  MAX(src.username_recovery_successes_on_chtr_network)/MAX(src.username_recovery_attempts_on_chtr_network) as percent_username_recovery_success_on_chtr_network,
  MAX(src.username_recovery_attempts_off_chtr_network) as username_recovery_attempts_off_chtr_network,
  MAX(src.username_recovery_successes_off_chtr_network) as username_recovery_successes_off_chtr_network,
  MAX(src.username_recovery_successes_off_chtr_network)/MAX(src.username_recovery_attempts_off_chtr_network) as percent_username_recovery_success_off_chtr_network,
  MAX(src.total_attempts_to_reset_password) as total_attempts_to_reset_password,
  MAX(src.total_successful_password_resets) as total_successful_password_resets,
  MAX(src.total_successful_password_resets)/MAX(src.total_attempts_to_reset_password) as percent_total_password_reset_success,
  MAX(src.attempts_to_reset_password_on_chtr_network) as attempts_to_reset_password_on_chtr_network,
  MAX(src.successful_password_resets_on_chtr_network) as successful_password_resets_on_chtr_network,
  MAX(src.successful_password_resets_on_chtr_network)/MAX(src.attempts_to_reset_password_on_chtr_network) as percent_password_reset_success_on_chtr_network,
  MAX(src.attempts_to_reset_password_off_chtr_network) as attempts_to_reset_password_off_chtr_network,
  MAX(src.successful_password_resets_off_chtr_network) as successful_password_resets_off_chtr_network,
  MAX(src.successful_password_resets_off_chtr_network)/MAX(src.attempts_to_reset_password_off_chtr_network) as percent_password_reset_success_off_chtr_network
FROM ${env:TMP_db}.sbnet_exec_monthly_agg_src_visits src
LEFT JOIN
  (SELECT
    lag_months.company,
    MAX(lag_months.hhs_logged_in_1_mo_lag) as hhs_logged_in_1_mo_lag,
    MAX(lag_months.hhs_logged_in_3_mo_lag) as hhs_logged_in_3_mo_lag
  FROM(
    SELECT
    company,
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
  WHERE company in ('L-CHTR','L-TWC','L-BHN')
    ) lag_months
  GROUP BY
    lag_months.company
  ) lag_months_grouped
ON src.company = lag_months_grouped.company
LEFT JOIN ${env:TMP_db}.sbnet_fed_ID_auths_monthly fid
ON src.company = fid.company
GROUP BY
  src.company,
  lag_months_grouped.hhs_logged_in_1_mo_lag,
  lag_months_grouped.hhs_logged_in_3_mo_lag
;
