USE ${env:ENVIRONMENT};

INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_instances
SELECT
  --Identifying data
  company AS company,
  'smb_sub_counts' AS source_file,
  --HHs
  CAST(NULL as INT) AS hhs_logged_in,
  subscriber_counts AS total_hhs,
  --Page Views
  CAST(NULL as INT) AS my_account_page_views,
  --Auth
  CAST(NULL as INT) AS total_login_attempts,
  CAST(NULL as INT) AS total_login_successes,
  --Self-Help Page
  CAST(NULL as INT) AS rescheduled_service_appointments,
  CAST(NULL as INT) AS cancelled_service_appointments,
  --Support
  CAST(NULL as INT) AS support_page_views,
  --OBP
  CAST(NULL as INT) AS online_statement_views,
  CAST(NULL as INT) AS one_time_payment_attempts,
  CAST(NULL as INT) AS one_time_payments,
  CAST(NULL as INT) AS auto_pay_setup_attempts,
  CAST(NULL as INT) AS auto_pay_setup_successes,
  --New Id Creation
  CAST(NULL as INT) AS total_account_creation_attempts,
  CAST(NULL as INT) AS total_new_accounts_created,
  CAST(NULL as INT) AS new_account_creation_attempts_on_chtr_network,
  CAST(NULL as INT) AS new_accounts_created_on_chtr_network,
  CAST(NULL as INT) AS new_account_creation_attempts_off_chtr_network,
  CAST(NULL as INT) AS new_accounts_created_off_chtr_network,
  --Sub-User Creation
  CAST(NULL as INT) AS total_sub_user_creation_attempts,
  CAST(NULL as INT) AS total_new_sub_users_created,
  CAST(NULL as INT) AS sub_user_creation_attempts_on_chtr_network,
  CAST(NULL as INT) AS sub_users_created_on_chtr_network,
  CAST(NULL as INT) AS sub_user_creation_attempts_off_chtr_network,
  CAST(NULL as INT) AS sub_users_created_off_chtr_network,
  --Username Recovery
  CAST(NULL as INT) AS total_username_recovery_attempts,
  CAST(NULL as INT) AS total_username_recovery_successes,
  CAST(NULL as INT) AS username_recovery_attempts_on_chtr_network,
  CAST(NULL as INT) AS username_recovery_successes_on_chtr_network,
  CAST(NULL as INT) AS username_recovery_attempts_off_chtr_network,
  CAST(NULL as INT) AS username_recovery_successes_off_chtr_network,
  --Password Reset
  CAST(NULL as INT) AS total_attempts_to_reset_password,
  CAST(NULL as INT) AS total_successful_password_resets,
  CAST(NULL as INT) AS attempts_to_reset_password_on_chtr_network,
  CAST(NULL as INT) AS successful_password_resets_on_chtr_network,
  CAST(NULL as INT) AS attempts_to_reset_password_off_chtr_network,
  CAST(NULL as INT) AS successful_password_resets_off_chtr_network
FROM ${env:TMP_db}.sbnet_exec_monthly_subscriber_counts_manual
WHERE
  year_month='${hiveconf:YEAR_MONTH}'
;


INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_visits
SELECT
  --Identifying data
  company AS company,
  'smb_sub_counts' AS source_file,
  --HHs
  CAST(NULL as INT) AS hhs_logged_in,
  subscriber_counts AS total_hhs,
  --Page Views
  CAST(NULL as INT) AS my_account_page_views,
  --Auth
  CAST(NULL as INT) AS total_login_attempts,
  CAST(NULL as INT) AS total_login_successes,
  --Self-Help Page
  CAST(NULL as INT) AS rescheduled_service_appointments,
  CAST(NULL as INT) AS cancelled_service_appointments,
  --Support
  CAST(NULL as INT) AS support_page_views,
  --OBP
  CAST(NULL as INT) AS online_statement_views,
  CAST(NULL as INT) AS one_time_payment_attempts,
  CAST(NULL as INT) AS one_time_payments,
  CAST(NULL as INT) AS one_time_payments_with_autopay_setup, -- added 2017-12-29 
  CAST(NULL as INT) AS auto_pay_setup_attempts,
  CAST(NULL as INT) AS auto_pay_setup_successes,
  --New Id Creation
  CAST(NULL as INT) AS total_account_creation_attempts,
  CAST(NULL as INT) AS total_new_accounts_created,
  CAST(NULL as INT) AS new_account_creation_attempts_on_chtr_network,
  CAST(NULL as INT) AS new_accounts_created_on_chtr_network,
  CAST(NULL as INT) AS new_account_creation_attempts_off_chtr_network,
  CAST(NULL as INT) AS new_accounts_created_off_chtr_network,
  --Sub-User Creation
  CAST(NULL as INT) AS total_sub_user_creation_attempts,
  CAST(NULL as INT) AS total_new_sub_users_created,
  CAST(NULL as INT) AS sub_user_creation_attempts_on_chtr_network,
  CAST(NULL as INT) AS sub_users_created_on_chtr_network,
  CAST(NULL as INT) AS sub_user_creation_attempts_off_chtr_network,
  CAST(NULL as INT) AS sub_users_created_off_chtr_network,
  --Username Recovery
  CAST(NULL as INT) AS total_username_recovery_attempts,
  CAST(NULL as INT) AS total_username_recovery_successes,
  CAST(NULL as INT) AS username_recovery_attempts_on_chtr_network,
  CAST(NULL as INT) AS username_recovery_successes_on_chtr_network,
  CAST(NULL as INT) AS username_recovery_attempts_off_chtr_network,
  CAST(NULL as INT) AS username_recovery_successes_off_chtr_network,
  --Password Reset
  CAST(NULL as INT) AS total_attempts_to_reset_password,
  CAST(NULL as INT) AS total_successful_password_resets,
  CAST(NULL as INT) AS attempts_to_reset_password_on_chtr_network,
  CAST(NULL as INT) AS successful_password_resets_on_chtr_network,
  CAST(NULL as INT) AS attempts_to_reset_password_off_chtr_network,
  CAST(NULL as INT) AS successful_password_resets_off_chtr_network
FROM ${env:TMP_db}.sbnet_exec_monthly_subscriber_counts_manual
WHERE
  year_month='${hiveconf:YEAR_MONTH}'
;
