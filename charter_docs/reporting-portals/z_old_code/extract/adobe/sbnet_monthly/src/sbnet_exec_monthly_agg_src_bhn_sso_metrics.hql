USE ${env:ENVIRONMENT};

INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_instances
SELECT
  --Identifying data
  company AS company,
  'bhn_sso_metrics' AS source_file,
  --HHs
  if(metric='unique_hhs_logged_in',value,null) AS hhs_logged_in, --HHs not tracked in BHN Adobe
  CAST(NULL as INT) AS total_hhs, --temporary until we receive SMB account flags in account feed
  --Page Views
  CAST(NULL as INT) AS my_account_page_views, --not tracked
  --Auth
  if(metric='login_attempts',value,null) AS total_login_attempts, --Tracked through SSO Profile DB
  if(metric='login_successes',value,null) AS total_login_successes, --Tracked through SSO Profile DB
  --Self-Help Page
  CAST(NULL as INT) AS rescheduled_service_appointments, --not tracked
  CAST(NULL as INT) AS cancelled_service_appointments, --not tracked
  --Support
  CAST(NULL as INT) AS support_page_views, --not tracked
  --OBP
  CAST(NULL as INT) AS online_statement_views, 
  CAST(NULL as INT) AS one_time_payment_attempts,
  CAST(NULL as INT) AS one_time_payments,
  CAST(NULL as INT) AS auto_pay_setup_attempts,
  CAST(NULL as INT) AS auto_pay_setup_successes,
  --New Id Creation
  CAST(NULL as INT) AS total_account_creation_attempts, --not tracked
  CAST(NULL as INT) AS total_new_accounts_created, --Tracked through SSO Profile DB
  CAST(NULL as INT) AS new_account_creation_attempts_on_chtr_network, --not tracked
  CAST(NULL as INT) AS new_accounts_created_on_chtr_network, --not tracked
  CAST(NULL as INT) AS new_account_creation_attempts_off_chtr_network, --not tracked
  CAST(NULL as INT) AS new_accounts_created_off_chtr_network, --not tracked
  --Sub-User Creation
  CAST(NULL as INT) AS total_sub_user_creation_attempts, --not tracked
  CAST(NULL as INT) AS total_new_sub_users_created, --not tracked
  CAST(NULL as INT) AS sub_user_creation_attempts_on_chtr_network, --not tracked
  CAST(NULL as INT) AS sub_users_created_on_chtr_network, --not tracked
  CAST(NULL as INT) AS sub_user_creation_attempts_off_chtr_network, --not tracked
  CAST(NULL as INT) AS sub_users_created_off_chtr_network, --not tracked
  --Username Recovery
  CAST(NULL as INT) AS total_username_recovery_attempts, --not tracked
  CAST(NULL as INT) AS total_username_recovery_successes, --not tracked
  CAST(NULL as INT) AS username_recovery_attempts_on_chtr_network, --not tracked
  CAST(NULL as INT) AS username_recovery_successes_on_chtr_network, --not tracked
  CAST(NULL as INT) AS username_recovery_attempts_off_chtr_network, --not tracked
  CAST(NULL as INT) AS username_recovery_successes_off_chtr_network, --not tracked
  --Password Reset
  CAST(NULL as INT) AS total_attempts_to_reset_password, --not tracked
  CAST(NULL as INT) AS total_successful_password_resets, --not tracked
  CAST(NULL as INT) AS attempts_to_reset_password_on_chtr_network, --not tracked
  CAST(NULL as INT) AS successful_password_resets_on_chtr_network, --not tracked
  CAST(NULL as INT) AS attempts_to_reset_password_off_chtr_network, --not tracked
  CAST(NULL as INT) AS successful_password_resets_off_chtr_network --not tracked
FROM ${env:TMP_db}.sbnet_exec_monthly_bhn_sso_metrics_manual
WHERE
  year_month='${hiveconf:YEAR_MONTH}'
;


INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_visits
SELECT
  --Identifying data
  company AS company,
  'bhn_sso_metrics' AS source_file,
  --HHs
  if(metric='unique_hhs_logged_in',value,null) AS hhs_logged_in, --HHs not tracked in BHN Adobe
  CAST(NULL as INT) AS total_hhs, --temporary until we receive SMB account flags in account feed
  --Page Views
  CAST(NULL as INT) AS my_account_page_views, --not tracked
  --Auth
  if(metric='login_attempts',value,null) AS total_login_attempts, --Tracked through SSO Profile DB
  if(metric='login_successes',value,null) AS total_login_successes, --Tracked through SSO Profile DB
  --Self-Help Page
  CAST(NULL as INT) AS rescheduled_service_appointments, --not tracked
  CAST(NULL as INT) AS cancelled_service_appointments, --not tracked
  --Support
  CAST(NULL as INT) AS support_page_views, --not tracked
  --OBP
  CAST(NULL as INT) AS online_statement_views, 
  CAST(NULL as INT) AS one_time_payment_attempts,
  CAST(NULL as INT) AS one_time_payments,
  CAST(NULL as INT) AS auto_pay_setup_attempts,
  CAST(NULL as INT) AS auto_pay_setup_successes,
  --New Id Creation
  CAST(NULL as INT) AS total_account_creation_attempts, --not tracked
  CAST(NULL as INT) AS total_new_accounts_created, --Tracked through SSO Profile DB
  CAST(NULL as INT) AS new_account_creation_attempts_on_chtr_network, --not tracked
  CAST(NULL as INT) AS new_accounts_created_on_chtr_network, --not tracked
  CAST(NULL as INT) AS new_account_creation_attempts_off_chtr_network, --not tracked
  CAST(NULL as INT) AS new_accounts_created_off_chtr_network, --not tracked
  --Sub-User Creation
  CAST(NULL as INT) AS total_sub_user_creation_attempts, --not tracked
  CAST(NULL as INT) AS total_new_sub_users_created, --not tracked
  CAST(NULL as INT) AS sub_user_creation_attempts_on_chtr_network, --not tracked
  CAST(NULL as INT) AS sub_users_created_on_chtr_network, --not tracked
  CAST(NULL as INT) AS sub_user_creation_attempts_off_chtr_network, --not tracked
  CAST(NULL as INT) AS sub_users_created_off_chtr_network, --not tracked
  --Username Recovery
  CAST(NULL as INT) AS total_username_recovery_attempts, --not tracked
  CAST(NULL as INT) AS total_username_recovery_successes, --not tracked
  CAST(NULL as INT) AS username_recovery_attempts_on_chtr_network, --not tracked
  CAST(NULL as INT) AS username_recovery_successes_on_chtr_network, --not tracked
  CAST(NULL as INT) AS username_recovery_attempts_off_chtr_network, --not tracked
  CAST(NULL as INT) AS username_recovery_successes_off_chtr_network, --not tracked
  --Password Reset
  CAST(NULL as INT) AS total_attempts_to_reset_password, --not tracked
  CAST(NULL as INT) AS total_successful_password_resets, --not tracked
  CAST(NULL as INT) AS attempts_to_reset_password_on_chtr_network, --not tracked
  CAST(NULL as INT) AS successful_password_resets_on_chtr_network, --not tracked
  CAST(NULL as INT) AS attempts_to_reset_password_off_chtr_network, --not tracked
  CAST(NULL as INT) AS successful_password_resets_off_chtr_network --not tracked
FROM ${env:TMP_db}.sbnet_exec_monthly_bhn_sso_metrics_manual
WHERE
  year_month='${hiveconf:YEAR_MONTH}'
;