USE ${env:ENVIRONMENT};

-- * NOTE: Login Attempts, Success, and OBP numbers have been manually adjusted for L-CHTR because of data source changes 
-- and process changes in June 2017.
-- As a result, running numbers from events tables will NOT match the aggregate totals for 2017-06. *

-- Truncate previous month's data from tmp table
TRUNCATE TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_instances;

-- Insert L-CHTR data into tmp table
INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_instances
SELECT
  --Identifying data
  'L-CHTR' AS company,
  'sbnet_events' AS source_file,
  --HHs
  CAST(NULL as INT) AS hhs_logged_in, --Tracked through Fed Id
  CAST(NULL as INT) AS total_hhs, --temporary until we receive SMB account flags in account feed
  --Page Views
  sum(if(message__category = 'Page View',1,0)) - sum(if(message__category = 'Page View' AND state__view__current_page__page_name = 'Login' AND state__view__current_page__section = 'Login' ,1,0)) AS my_account_page_views,
  --Auth
  CAST(NULL as INT) AS total_login_attempts,
  CAST(NULL as INT) AS total_login_successes, --Tracked through Fed Id
  --Self-Help Page
  sum(if(message__name = 'Confirm Reschedule Appointment' and state__view__current_page__section = 'Appointment Tracking',1,0)) AS rescheduled_service_appointments,
  sum(if(message__name = 'Confirm Cancel Appointment' and state__view__current_page__section = 'Appointment Tracking',1,0)) AS cancelled_service_appointments,
  --Support
  sum(if(message__category = 'Page View' and state__view__current_page__section = 'Support',1,0)) AS support_page_views,
  --OBP
  sum(if(state__view__current_page__sub_section = 'View Statements',1,0)) + sum(if(message__category = 'Custom Link' AND message__name = 'Select Statement',1,0)) AS online_statement_views, --new as of 2017-08-21 (instances)
  sum(if(message__name = 'Complete Payment' , 1,0)) AS one_time_payment_attempts, --new as of 2017-08-21 (instances)
  sum(if(state__view__current_page__page_name = 'Confirm Payment' and state__view__previous_page__page_name = 'Verify One Time Payment', 1, 0)) AS one_time_payments, --new as of 2017-08-21 (instances)
  sum(if(message__name = 'Save Auto-Pay', 1, 0)) AS auto_pay_setup_attempts, --new as of 2017-08-21 (instances)
  sum(if(state__view__current_page__page_name = 'Confirm  Auto Pay Setup' and state__view__previous_page__page_name = 'Verify Auto Pay Setup', 1, 0)) AS auto_pay_setup_successes, --new as of 2017-08-21 (instances)
  --New Id Creation
  sum(if(message__name = 'Create Account Register',1,0)) AS total_account_creation_attempts,
  sum(if(message__name = 'Create Account Summary',1,0)) AS total_new_accounts_created,
  sum(if(message__name = 'Create Account Register' and visit__isp__isp LIKE 'charter%',1,0)) AS new_account_creation_attempts_on_chtr_network,
  sum(if(message__name = 'Create Account Summary' and visit__isp__isp LIKE 'charter%',1,0)) AS new_accounts_created_on_chtr_network,
  sum(if(message__name = 'Create Account Register',1,0)) - sum(if(message__name = 'Create Account Register' and visit__isp__isp LIKE 'charter%',1,0)) AS new_account_creation_attempts_off_chtr_network,
  sum(if(message__name = 'Create Account Summary',1,0)) - sum(if(message__name = 'Create Account Summary' and visit__isp__isp LIKE 'charter%',1,0)) AS new_accounts_created_off_chtr_network,
  --Sub-User Creation
  sum(if(message__name = 'Add New User' and state__view__current_page__page_name = 'Manage Users Page',1,0)) AS total_sub_user_creation_attempts,
  sum(if(message__name = 'Add New User Confirm Info Next',1,0)) AS total_new_sub_users_created,
  sum(if(message__name = 'Add New User' and state__view__current_page__page_name = 'Manage Users Page' and visit__isp__isp LIKE 'charter%',1,0)) AS sub_user_creation_attempts_on_chtr_network,
  sum(if(message__name = 'Add New User Confirm Info Next' and visit__isp__isp LIKE 'charter%',1,0)) AS sub_users_created_on_chtr_network,
  sum(if(message__name = 'Add New User' and state__view__current_page__page_name = 'Manage Users Page',1,0)) - sum(if(message__name = 'Add New User' and state__view__current_page__page_name = 'Manage Users Page' and visit__isp__isp LIKE 'charter%',1,0)) AS sub_user_creation_attempts_off_chtr_network,
  sum(if(message__name = 'Add New User Confirm Info Next',1,0)) - sum(if(message__name = 'Add New User Confirm Info Next' and visit__isp__isp LIKE 'charter%',1,0)) AS sub_users_created_off_chtr_network,
  --Username Recovery
  sum(if(message__name = 'Username Recovery Next' and state__view__current_page__sub_section = 'Account Recovery - Recovery Options',1,0)) AS total_username_recovery_attempts,
  sum(if(message__name = 'Account Recovery Return to Login' and state__view__current_page__sub_section = 'Account Recovery Complete',1,0)) AS total_username_recovery_successes,
  sum(if(message__name = 'Username Recovery Next' and state__view__current_page__sub_section = 'Account Recovery - Recovery Options' and visit__isp__isp LIKE 'charter%',1,0)) AS username_recovery_attempts_on_chtr_network,
  sum(if(message__name = 'Account Recovery Return to Login' and state__view__current_page__sub_section = 'Account Recovery Complete' and visit__isp__isp LIKE 'charter%',1,0)) AS username_recovery_successes_on_chtr_network,
  sum(if(message__name = 'Username Recovery Next' and state__view__current_page__sub_section = 'Account Recovery - Recovery Options',1,0)) - sum(if(message__name = 'Username Recovery Next' and state__view__current_page__sub_section = 'Account Recovery - Recovery Options' and visit__isp__isp LIKE 'charter%',1,0)) AS username_recovery_attempts_off_chtr_network,
  sum(if(message__name = 'Account Recovery Return to Login' and state__view__current_page__sub_section = 'Account Recovery Complete',1,0)) - sum(if(message__name = 'Account Recovery Return to Login' and state__view__current_page__sub_section = 'Account Recovery Complete' and visit__isp__isp LIKE 'charter%',1,0)) AS username_recovery_successes_off_chtr_network,
  --Password Reset
  sum(if(message__name = 'Password Recovery Next' and state__view__current_page__sub_section = 'Account Recovery - Recovery Options',1,0)) AS total_attempts_to_reset_password,
  sum(if(message__name LIKE 'Account Recovery Password Reset Complete%',1,0)) AS total_successful_password_resets,
  sum(if(message__name = 'Password Recovery Next' and state__view__current_page__sub_section = 'Account Recovery - Recovery Options' and visit__isp__isp LIKE 'charter%',1,0)) AS attempts_to_reset_password_on_chtr_network,
  sum(if(message__name LIKE 'Account Recovery Password Reset Complete%' and visit__isp__isp LIKE 'charter%',1,0)) AS successful_password_resets_on_chtr_network,
  sum(if(message__name = 'Password Recovery Next' and state__view__current_page__sub_section = 'Account Recovery - Recovery Options',1,0)) - sum(if(message__name = 'Password Recovery Next' and state__view__current_page__sub_section = 'Account Recovery - Recovery Options' and visit__isp__isp LIKE 'charter%',1,0)) AS attempts_to_reset_password_off_chtr_network,
  sum(if(message__name LIKE 'Account Recovery Password Reset Complete%',1,0)) - sum(if(message__name LIKE 'Account Recovery Password Reset Complete%' and visit__isp__isp LIKE 'charter%',1,0)) AS successful_password_resets_off_chtr_network
FROM 
  sbnet_events
WHERE 
  (partition_date_hour_utc >= concat('${hiveconf:MONTH_START_DATE}', '_', '${env:TZ_OFFSET_DEN}') 
    and
    partition_date_hour_utc < concat('${hiveconf:MONTH_END_DATE}', '_', '${env:TZ_OFFSET_DEN}'));


-- Insert L-TWC My Account data into tmp table
INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_instances
SELECT
  --Identifying data
  'L-TWC' AS company,
  'twcmyacct_events' AS source_file,
  --HHs
  CAST(NULL as INT) AS hhs_logged_in, --Tracked through Fed Id
  CAST(NULL as INT) AS total_hhs, --temporary until we receive SMB account flags in account feed
  --Page Views
  sum(if(message__category = 'Page View',1,0)) - sum(if(message__category = 'Page View' AND state__view__current_page__page_name = 'my account > login',1,0)) AS my_account_page_views,
  --Auth
  CAST(NULL as INT) AS total_login_attempts,
  CAST(NULL as INT) AS total_login_successes, --Tracked through Fed Id
  --Self-Help Page
  sum(if(state__view__current_page__elements__name = 'my account > support > reschedule appointment submit',1,0)) AS rescheduled_service_appointments,
  sum(if(state__view__current_page__elements__name = 'my account > support > cancel appointment submit',1,0)) AS cancelled_service_appointments,
  --Support
  CAST(null as INT) AS support_page_views,
  --OBP
  sum(if(state__view__current_page__elements__name LIKE '%billing > statements: statement download%',1,0)) AS online_statement_views,
  CAST(null as INT) AS one_time_payment_attempts,
  sum(if(state__view__current_page__elements__name like '%step 4%' and (state__view__current_page__elements__name like '%fdp%' or state__view__current_page__elements__name like '%one time%'),1,0)) AS one_time_payments,
  CAST(null as INT) AS auto_pay_setup_attempts,
  sum(if(state__view__current_page__elements__name like '%step 4%' and state__view__current_page__elements__name like '%recurring%',1,0)) AS auto_pay_setup_successes,
  --New Id Creation
  CAST(NULL as INT) AS total_account_creation_attempts,
  CAST(NULL as INT) AS total_new_accounts_created,
  CAST(NULL as INT) AS new_account_creation_attempts_on_chtr_network,
  CAST(NULL as INT) AS new_accounts_created_on_chtr_network,
  CAST(NULL as INT) AS new_account_creation_attempts_off_chtr_network,
  CAST(NULL as INT) AS new_accounts_created_off_chtr_network,
  --Sub-User Creation
  sum(if(state__view__current_page__elements__name = 'my account > users > add user' and state__view__current_page__page_name = 'my account > users',1,0)) AS total_sub_user_creation_attempts,
  sum(if(state__view__current_page__elements__name = 'my account > users > add user save' and state__view__current_page__page_name = 'my account > users',1,0)) AS total_new_sub_users_created,
  sum(if(state__view__current_page__elements__name = 'my account > users > add user' and state__view__current_page__page_name = 'my account > users' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS sub_user_creation_attempts_on_chtr_network,
  sum(if(state__view__current_page__elements__name = 'my account > users > add user save' and state__view__current_page__page_name = 'my account > users' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS sub_users_created_on_chtr_network,
  sum(if(state__view__current_page__elements__name = 'my account > users > add user' and state__view__current_page__page_name = 'my account > users',1,0)) - sum(if(state__view__current_page__elements__name = 'my account > users > add user' and state__view__current_page__page_name = 'my account > users' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS sub_user_creation_attempts_off_chtr_network,
  sum(if(state__view__current_page__elements__name = 'my account > users > add user save' and state__view__current_page__page_name = 'my account > users',1,0)) - sum(if(state__view__current_page__elements__name = 'my account > users > add user save' and state__view__current_page__page_name = 'my account > users' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS sub_users_created_off_chtr_network,
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
FROM 
  twcmyacct_events
WHERE 
  (partition_date_hour_utc >= concat('${hiveconf:MONTH_START_DATE}', '_', '${env:TZ_OFFSET_DEN}') 
    and
    partition_date_hour_utc < concat('${hiveconf:MONTH_END_DATE}', '_', '${env:TZ_OFFSET_DEN}'));


-- Insert L-TWC Business Global data into tmp table
INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_instances
SELECT
  --Identifying data
  'L-TWC' AS company,
  'twcbusglobal_events' AS source_file,
  --HHs
  CAST(NULL as INT) AS hhs_logged_in, --Tracked through Fed Id
  CAST(NULL as INT) AS total_hhs, 
  --Page Views
  CAST(NULL as INT) AS my_account_page_views,
  --Auth
  CAST(NULL as INT) AS total_login_attempts,
  CAST(NULL as INT) AS total_login_successes, --Tracked through Fed Id
  --Self-Help Page
  CAST(NULL as INT) AS rescheduled_service_appointments,
  CAST(NULL as INT) AS cancelled_service_appointments,
  --Support
  sum(if(message__category = 'Page View' and lower(state__view__current_page__page_name) like '%support%',1,0)) AS support_page_views,
  --OBP
  CAST(NULL as INT) AS online_statement_views,
  CAST(NULL as INT) AS one_time_payment_attempts,
  CAST(NULL as INT) AS one_time_payments,
  CAST(NULL as INT) AS auto_pay_setup_attempts,
  CAST(NULL as INT) AS auto_pay_setup_successes,
  --New Id Creation
  sum(if(array_contains(message__name,'CLA: Create Your Account') and state__view__current_page__page_id like 'https://registration.timewarnercable.com/businessclass%',1,0)) AS total_account_creation_attempts,
  sum(if(array_contains(message__name,'CLA: Confirmation') and state__view__current_page__page_id like 'https://registration.timewarnercable.com/businessclass%',1,0)) AS total_new_accounts_created,
  sum(if(array_contains(message__name,'CLA: Create Your Account') and state__view__current_page__page_id like 'https://registration.timewarnercable.com/businessclass%' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS new_account_creation_attempts_on_chtr_network,
  sum(if(array_contains(message__name,'CLA: Confirmation') and state__view__current_page__page_id like 'https://registration.timewarnercable.com/businessclass%' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS new_accounts_created_on_chtr_network,
  sum(if(array_contains(message__name,'CLA: Create Your Account') and state__view__current_page__page_id like 'https://registration.timewarnercable.com/businessclass%',1,0)) - sum(if(array_contains(message__name,'CLA: Create Your Account') and state__view__current_page__page_id like 'https://registration.timewarnercable.com/businessclass%' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS new_account_creation_attempts_off_chtr_network,
  sum(if(array_contains(message__name,'CLA: Confirmation') and state__view__current_page__page_id like 'https://registration.timewarnercable.com/businessclass%',1,0)) - sum(if(array_contains(message__name,'CLA: Confirmation') and state__view__current_page__page_id like 'https://registration.timewarnercable.com/businessclass%' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS new_accounts_created_off_chtr_network,
  --Sub-User Creation
  CAST(NULL as INT) AS total_sub_user_creation_attempts,
  CAST(NULL as INT) AS total_new_sub_users_created,
  CAST(NULL as INT) AS sub_user_creation_attempts_on_chtr_network,
  CAST(NULL as INT) AS sub_users_created_on_chtr_network,
  CAST(NULL as INT) AS sub_user_creation_attempts_off_chtr_network,
  CAST(NULL as INT) AS sub_users_created_off_chtr_network,
  --Username Recovery
  sum(if(state__view__current_page__page_name = 'bc > forgot username > step 1' and state__view__previous_page__page_name like 'my account%',1,0)) AS total_username_recovery_attempts,
  sum(if(state__view__current_page__page_name = 'bc > forgot username> email sent',1,0)) AS total_username_recovery_successes,
  sum(if(state__view__current_page__page_name = 'bc > forgot username > step 1' and state__view__previous_page__page_name like 'my account%' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com') ,1,0)) AS username_recovery_attempts_on_chtr_network,
  sum(if(state__view__current_page__page_name = 'bc > forgot username> email sent' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS username_recovery_successes_on_chtr_network,
  sum(if(state__view__current_page__page_name = 'bc > forgot username > step 1' and state__view__previous_page__page_name like 'my account%',1,0)) - sum(if(state__view__current_page__page_name = 'bc > forgot username > step 1' and state__view__previous_page__page_name like 'my account%' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com') ,1,0)) AS username_recovery_attempts_off_chtr_network,
  sum(if(state__view__current_page__page_name = 'bc > forgot username> email sent',1,0)) - sum(if(state__view__current_page__page_name = 'bc > forgot username> email sent' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS username_recovery_successes_off_chtr_network,
  --Password Reset
  sum(if(state__view__current_page__page_name = 'bc > forgot password > step 1' and state__view__previous_page__page_name like 'my account%',1,0)) AS total_attempts_to_reset_password,
  sum(if(state__view__current_page__page_name = 'bc > forgot password > change password confirmation',1,0)) AS total_successful_password_resets,
  sum(if(state__view__current_page__page_name = 'bc > forgot password > step 1' and state__view__previous_page__page_name like 'my account%' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS attempts_to_reset_password_on_chtr_network,
  sum(if(state__view__current_page__page_name = 'bc > forgot password > change password confirmation' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS successful_password_resets_on_chtr_network,
  sum(if(state__view__current_page__page_name = 'bc > forgot password > step 1' and state__view__previous_page__page_name like 'my account%',1,0)) - sum(if(state__view__current_page__page_name = 'bc > forgot password > step 1' and state__view__previous_page__page_name like 'my account%' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS attempts_to_reset_password_off_chtr_network,
  sum(if(state__view__current_page__page_name = 'bc > forgot password > change password confirmation',1,0)) - sum(if(state__view__current_page__page_name = 'bc > forgot password > change password confirmation' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) AS successful_password_resets_off_chtr_network
FROM 
  twcbusglobal_events
WHERE 
  (partition_date_hour_utc >= concat('${hiveconf:MONTH_START_DATE}', '_', '${env:TZ_OFFSET_DEN}') 
    and
    partition_date_hour_utc < concat('${hiveconf:MONTH_END_DATE}', '_', '${env:TZ_OFFSET_DEN}'));


-- Insert L-BHN My Services data into tmp table
INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_instances
SELECT
  --Identifying data
  'L-BHN' AS company,
  'bhnmyservices_events' AS source_file,
  --HHs
  CAST(NULL as INT) AS hhs_logged_in, --Tracked through Fed Id
  CAST(NULL as INT) AS total_hhs, --temporary until we receive SMB account flags in account feed
  --Page Views
  SUM(IF(message__category='Page View' 
    AND lower(state__view__current_page__page_id) NOT LIKE '%bhnbusiness%'
    AND lower(state__view__current_page__page_id) NOT LIKE '%login%'
    AND lower(state__view__current_page__page_id) NOT LIKE '%businesssolutions.brighthouse.com/home%'
    AND lower(state__view__current_page__page_id) NOT LIKE '%businesssolutions.brighthouse.com/content/mobile/business/home%',1,0)) AS my_account_page_views,
  --Auth
  CAST(NULL as INT) AS total_login_attempts, 
  CAST(NULL as INT) AS total_login_successes, --Tracked through Fed Id
  --Self-Help Page
  CAST(NULL as INT) AS rescheduled_service_appointments, --not tracked
  CAST(NULL as INT) AS cancelled_service_appointments, --not tracked
  --Support
  CAST(NULL as INT) AS support_page_views,
  --OBP
  CAST(NULL as INT) AS online_statement_views, --tracked in BHN Bill Pay Table, Query Below
  CAST(NULL as INT) AS one_time_payment_attempts, --tracked in BHN Bill Pay Table, Query Below
  CAST(NULL as INT) AS one_time_payments, --tracked in BHN Bill Pay Table, Query Below
  CAST(NULL as INT) AS auto_pay_setup_attempts, --tracked in BHN Bill Pay Table, Query Below
  CAST(NULL as INT) AS auto_pay_setup_successes, --tracked in BHN Bill Pay Table, Query Below
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
FROM 
  bhnmyservices_events
WHERE 
  (partition_date_hour_utc >= concat('${hiveconf:MONTH_START_DATE}', '_', '${env:TZ_OFFSET_DEN}') 
    and
    partition_date_hour_utc < concat('${hiveconf:MONTH_END_DATE}', '_', '${env:TZ_OFFSET_DEN}')
  and state__view__current_page__page_type = 'small-medium');


-- Insert L-BHN My Services data into tmp table
INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_instances
SELECT
  --Identifying data
  'L-BHN' AS company,
  'bhn_bill_pay_events' AS source_file,
  --HHs
  CAST(NULL as INT) AS hhs_logged_in, --Tracked through Fed Id
  CAST(NULL as INT) AS total_hhs, --temporary until we receive SMB account flags in account feed
  --Page Views
  CAST(NULL as INT) AS my_account_page_views, --not tracked
  --Auth
  CAST(NULL as INT) AS total_login_attempts, 
  CAST(NULL as INT) AS total_login_successes, --Tracked through Fed Id
  --Self-Help Page
  CAST(NULL as INT) AS rescheduled_service_appointments, --not tracked
  CAST(NULL as INT) AS cancelled_service_appointments, --not tracked
  --Support
  CAST(NULL as INT) AS support_page_views, --not tracked
  --OBP
  sum(if(array_contains(message__feature__name,'Custom Event 11') AND state__view__current_page__page_type='SMB',1,0)) AS online_statement_views, 
  sum(if(array_contains(message__feature__name,'Custom Event 36') AND state__view__current_page__page_type='SMB',1,0)) AS one_time_payment_attempts,
  sum(if(array_contains(message__feature__name,'Custom Event 31') AND state__view__current_page__page_type='SMB',1,0)) AS one_time_payments,
  sum(if(array_contains(message__feature__name,'Custom Event 19') AND state__view__current_page__page_type='SMB',1,0)) AS auto_pay_setup_attempts,
  sum(if(array_contains(message__feature__name,'Custom Event 24') AND state__view__current_page__page_type='SMB',1,0)) AS auto_pay_setup_successes,
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
FROM 
  bhn_bill_pay_events
WHERE 
  (partition_date_hour_utc >= concat('${hiveconf:MONTH_START_DATE}', '_', '${env:TZ_OFFSET_DEN}') 
    and
    partition_date_hour_utc < concat('${hiveconf:MONTH_END_DATE}', '_', '${env:TZ_OFFSET_DEN}'));
