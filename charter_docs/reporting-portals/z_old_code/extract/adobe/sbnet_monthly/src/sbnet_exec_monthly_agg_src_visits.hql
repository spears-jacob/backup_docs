USE ${env:ENVIRONMENT};

-- * NOTE: Login Attempts, Success, and OBP numbers have been manually adjusted for L-CHTR because of data source changes
-- and process changes in June 2017.
-- As a result, running numbers from events tables will NOT match the aggregate totals for 2017-06. *

-- Truncate previous month's data from tmp table
TRUNCATE TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_visits;

-- Insert L-CHTR data into tmp table
INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_visits
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
  CAST(NULL as INT) AS total_login_successes, --Tracked with Fed Id
  --Self-Help Page
  size(collect_set(if(message__name = 'Confirm Reschedule Appointment' and state__view__current_page__section = 'Appointment Tracking',visit__visit_id,NULL))) AS rescheduled_service_appointments,
  size(collect_set(if(message__name = 'Confirm Cancel Appointment' and state__view__current_page__section = 'Appointment Tracking',visit__visit_id,NULL))) AS cancelled_service_appointments,
  --Support
  sum(if(message__category = 'Page View' and state__view__current_page__section = 'Support',1,0)) AS support_page_views,
  --OBP
  SIZE(COLLECT_SET(CASE WHEN state__view__current_page__page_name = 'Statements' THEN visit__visit_id ELSE NULL END))
      + SIZE(COLLECT_SET(CASE WHEN message__category = 'Custom Link' AND message__name = 'Download Statement' THEN visit__visit_id ELSE NULL END)) AS online_statement_views, --new as of 2017-10-17 (visits)
  size(collect_set(if(message__name = 'Complete Payment' ,visit__visit_id,NULL))) AS one_time_payment_attempts, --new as of 2017-08-21 (visits)
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'pay-bill.onetime-confirmation', visit__visit_id, NULL))) AS one_time_payments, --new as of 2017-11-22 (visits)
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'pay-bill.onetime-confirmation-with-autopay-enrollment', visit__visit_id, NULL))) AS one_time_payments_with_autopay_setup, --new as of 2017-11-22 (visits)
  size(collect_set(if(message__name = 'Save Auto-Pay',visit__visit_id,NULL))) AS auto_pay_setup_attempts, --new as of 2017-08-21 (visits)
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'pay-bill.autopay-enrollment-confirmation', visit__visit_id, NULL))) AS auto_pay_setup_successes, --new as of 2017-11-22 (visits)
  --New Id Creation
  size(collect_set(if(message__name = 'Create Account Register',visit__visit_id,NULL))) AS total_account_creation_attempts,
  size(collect_set(if(message__name = 'Create Account Summary',visit__visit_id,NULL))) AS total_new_accounts_created,
  size(collect_set(if(message__name = 'Create Account Register' and visit__isp__isp LIKE 'charter%',visit__visit_id,NULL))) AS new_account_creation_attempts_on_chtr_network,
  size(collect_set(if(message__name = 'Create Account Summary' and visit__isp__isp LIKE 'charter%',visit__visit_id,NULL))) AS new_accounts_created_on_chtr_network,
  size(collect_set(if(message__name = 'Create Account Register',visit__visit_id,NULL))) - size(collect_set(if(message__name = 'Create Account Register' and visit__isp__isp LIKE 'charter%',visit__visit_id,NULL))) AS new_account_creation_attempts_off_chtr_network,
  size(collect_set(if(message__name = 'Create Account Summary',visit__visit_id,NULL))) - size(collect_set(if(message__name = 'Create Account Summary' and visit__isp__isp LIKE 'charter%',visit__visit_id,NULL))) AS new_accounts_created_off_chtr_network,
  --Sub-User Creation
  sum(if(message__name = 'Add New User' and message__category = 'Custom Link',1,0)) AS total_sub_user_creation_attempts,
  sum(if(state__view__current_page__page_name = 'Manage Users Page' AND visit__application_details__referrer_link = 'Add New User Confirm Info Next',1,0)) AS total_new_sub_users_created,
  sum(if(message__name = 'Add New User' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*',1,0)) AS sub_user_creation_attempts_on_chtr_network,
  sum(if(state__view__current_page__page_name = 'Manage Users Page' AND visit__application_details__referrer_link = 'Add New User Confirm Info Next' and visit__isp__isp RLIKE 'charter.*',1,0)) AS sub_users_created_on_chtr_network,
  Sum(if(message__name = 'Add New User' and message__category = 'Custom Link',1,0)) - sum(if(message__name = 'Add New User' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*',1,0)) AS sub_user_creation_attempts_off_chtr_network,
  sum(if(state__view__current_page__page_name = 'Manage Users Page' AND visit__application_details__referrer_link = 'Add New User Confirm Info Next',1,0)) - sum(if(state__view__current_page__page_name = 'Manage Users Page' AND visit__application_details__referrer_link = 'Add New User Confirm Info Next' and visit__isp__isp RLIKE 'charter.*',1,0)) AS sub_users_created_off_chtr_network,
  --Username Recovery
  size(collect_set(if(message__name = 'Username Recovery Next' and message__category = 'Custom Link', visit__visit_id,NULL))) AS total_username_recovery_attempts,
  size(collect_set(if(message__name = 'Account Recovery Return to Login' and message__category = 'Custom Link', visit__visit_id,NULL))) AS total_username_recovery_successes,
  size(collect_set(if(message__name = 'Username Recovery Next' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS username_recovery_attempts_on_chtr_network,
  size(collect_set(if(message__name = 'Account Recovery Return to Login' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS username_recovery_successes_on_chtr_network,
  size(collect_set(if(message__name = 'Username Recovery Next' and message__category = 'Custom Link', visit__visit_id,NULL))) - size(collect_set(if(message__name = 'Username Recovery Next' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS username_recovery_attempts_off_chtr_network,
  size(collect_set(if(message__name = 'Account Recovery Return to Login' and message__category = 'Custom Link', visit__visit_id,NULL))) - size(collect_set(if(message__name = 'Account Recovery Return to Login' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS username_recovery_successes_off_chtr_network,
  --Password Reset
  size(collect_set(if(message__name = 'Password Recovery Next' and message__category = 'Custom Link', visit__visit_id, NULL))) AS total_attempts_to_reset_password,
  size(collect_set(if(message__name RLIKE 'Account Recovery Password Reset Complete.*' AND message__category = 'Custom Link', visit__visit_id, NULL))) AS total_successful_password_resets,
  size(collect_set(if(message__name = 'Password Recovery Next' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id, NULL))) AS attempts_to_reset_password_on_chtr_network,
  size(collect_set(if(message__name RLIKE 'Account Recovery Password Reset Complete.*' AND message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS successful_password_resets_on_chtr_network,
  size(collect_set(if(message__name = 'Password Recovery Next' and message__category = 'Custom Link', visit__visit_id, NULL))) - size(collect_set(if(message__name = 'Password Recovery Next' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id, NULL))) AS attempts_to_reset_password_off_chtr_network,
  size(collect_set(if(message__name RLIKE 'Account Recovery Password Reset Complete.*' AND message__category = 'Custom Link', visit__visit_id, NULL))) - size(collect_set(if(message__name RLIKE 'Account Recovery Password Reset Complete.*' AND message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS successful_password_resets_off_chtr_network
FROM
  sbnet_events
WHERE
  (partition_date_hour_utc >= concat('${hiveconf:MONTH_START_DATE}', '_', '${env:TZ_OFFSET_DEN}')
    and
    partition_date_hour_utc < concat('${hiveconf:MONTH_END_DATE}', '_', '${env:TZ_OFFSET_DEN}'));


-- Insert L-TWC My Account data into tmp table
-- **Maybe we should delete this step** --
INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_visits
SELECT
  --Identifying data
  'L-TWC' AS company,
  'twcmyacct_events' AS source_file,
  --HHs
  CAST(NULL as INT) AS hhs_logged_in, --Tracked with Fed Id
  CAST(NULL as INT) AS total_hhs, --temporary until we receive SMB account flags in account feed
  --Page Views
  CAST(NULL as INT) AS my_account_page_views,
  --Auth
  sum(if(array_contains(message__name,'My Account Login'),1,0)) + sum(if(state__view__current_page__page_id like '%loginError%',1,0)) AS total_login_attempts,
  CAST(NULL as INT) AS total_login_successes, --Tracked with Fed Id
  --Self-Help Page
  CAST(NULL as INT) AS rescheduled_service_appointments,
  CAST(NULL as INT) AS cancelled_service_appointments,
  --Support
  CAST(NULL as INT) AS support_page_views,
  --OBP
  CAST(NULL as INT) AS online_statement_views,
  CAST(null as INT) AS one_time_payment_attempts,
  CAST(null as INT) AS one_time_payments,
  CAST(null as INT) AS one_time_payments_with_autopay_setup, --new as of 2017-11-22 (visits)
  CAST(null as INT) AS auto_pay_setup_attempts,
  CAST(null as INT) AS auto_pay_setup_successes,
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
FROM
  twcmyacct_events
WHERE
  (partition_date_hour_utc >= concat('${hiveconf:MONTH_START_DATE}', '_', '${env:TZ_OFFSET_DEN}')
    and
    partition_date_hour_utc < concat('${hiveconf:MONTH_END_DATE}', '_', '${env:TZ_OFFSET_DEN}'));


-- Insert L-TWC Business Global data into tmp table
INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_visits
SELECT
  --Identifying data
  'L-TWC' AS company,
  'twcbusglobal_events' AS source_file,
  --HHs
  CAST(NULL as INT) AS hhs_logged_in, --Tracked through Fed Id
  CAST(NULL as INT) AS total_hhs,
  --Page Views
  SUM(IF(message__triggered_by IN('cla','my account') AND message__category = 'Page View',1, 0)) AS my_account_page_views, --updated 2017-12-26 DPrince
  --Auth
  CAST(NULL as INT) AS total_login_attempts,
  CAST(NULL as INT) AS total_login_successes, --Tracked through Fed Id
  --Self-Help Page
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > support > reschedule appointment submit' ,visit__visit_id, NULL))) AS rescheduled_service_appointments, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > support > cancel appointment submit' ,visit__visit_id, NULL))) AS cancelled_service_appointments, -- updated 2017-12-26 DPrince
  --Support
  SUM(IF(((state__view__current_page__page_name NOT RLIKE '.*channelpartners.*|.*channel partners.*|.*enterprise.*')
          AND state__view__current_page__page_name RLIKE '.*support.*' AND message__category = 'Page View' )
          OR (message__triggered_by <> 'enterprise'
          AND message__triggered_by <> 'channel partners'
          AND state__view__current_page__page_name RLIKE '.*faq.*' AND message__category = 'Page View'),1,0)) AS support_page_views, --updated 2017-12-26 DPrince
  --OBP
  SUM(IF(visit__device__device_type RLIKE '.*220.*' AND state__view__current_page__elements__name = 'my account > billing > statements: statement download', 1, 0 )) AS online_statement_views, --updated 2017-12-26 DPrince
  CAST(NULL as INT) AS one_time_payment_attempts,
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name RLIKE '.*step 4.*'
      AND (state__view__current_page__elements__name RLIKE '.*fdp.*|.*one time.*') ,visit__visit_id, NULL))) AS one_time_payments, --updated 2017-12-26 DPrince
  CAST(null as INT) AS one_time_payments_with_autopay_setup, --new as of 2017-11-22 (visits)
  CAST(NULL as INT) AS auto_pay_setup_attempts,
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name RLIKE '.*step 4.*'
      AND (state__view__current_page__elements__name RLIKE '.*recurring.*') ,visit__visit_id, NULL))) AS auto_pay_setup_successes, --updated 2017-12-26 DPrince
  --New Id Creation
  ---total
  SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*249.*', visit__visit_id, NULL))) AS total_account_creation_attempts, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*253.*', visit__visit_id, NULL))) AS total_new_accounts_created, --updated 2017-12-26 Dprince
  ---on net
  SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*249.*' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS new_account_creation_attempts_on_chtr_network, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*253.*' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS new_accounts_created_on_chtr_network, --updated 2017-12-26 DPrince
  ---off net
  SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*249.*' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS new_account_creation_attempts_off_chtr_network, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*253.*' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS new_accounts_created_off_chtr_network, --updated 2017-12-26 DPrince
  --Sub-User Creation
  ---total
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name =  'my account > users > add user', visit__visit_id, NULL))) AS total_sub_user_creation_attempts, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > users > add user save', visit__visit_id, NULL))) AS total_new_sub_users_created, --updated 2017-12-26 DPrince
  ---on net
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name =  'my account > users > add user' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS sub_user_creation_attempts_on_chtr_network, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > users > add user save' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS sub_users_created_on_chtr_network, --updated 2017-12-26 DPrince
  ---off net
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name =  'my account > users > add user' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS sub_user_creation_attempts_off_chtr_network, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > users > add user save' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS sub_users_created_off_chtr_network, --updated 2017-12-26 DPrince
  --Username Recovery
  ---total
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username > step 1', visit__visit_id, NULL))) AS total_username_recovery_attempts, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username> email sent', visit__visit_id, NULL))) AS total_username_recovery_successes, --updated 2017-12-26 DPrince
  ---on net
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username > step 1' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS username_recovery_attempts_on_chtr_network, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username> email sent' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS username_recovery_successes_on_chtr_network, --updated 2017-12-26 DPrince
  ---off net
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username > step 1' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS username_recovery_attempts_off_chtr_network, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username> email sent' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS username_recovery_successes_off_chtr_network, --updated 2017-12-26 DPrince
  --Password Reset
  ---total
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name RLIKE ('bc > forgot password > step ?1'), visit__visit_id, NULL))) AS total_attempts_to_reset_password, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot password > change password confirmation', visit__visit_id, NULL))) AS total_successful_password_resets, --updated 2017-12-26 DPrince
  --- on net
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name RLIKE ('bc > forgot password > step ?1') AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS attempts_to_reset_password_on_chtr_network, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot password > change password confirmation' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS successful_password_resets_on_chtr_network, --updated 2017-12-26 DPrince
  --- off net
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name RLIKE ('bc > forgot password > step ?1') AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS attempts_to_reset_password_off_chtr_network, --updated 2017-12-26 DPrince
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot password > change password confirmation' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS successful_password_resets_off_chtr_network --updated 2017-12-26 DPrince
FROM
  twcbusglobal_events
WHERE
  (partition_date_hour_utc >= concat('${hiveconf:MONTH_START_DATE}', '_', '${env:TZ_OFFSET_DEN}')
    and
    partition_date_hour_utc < concat('${hiveconf:MONTH_END_DATE}', '_', '${env:TZ_OFFSET_DEN}'))--
    ;


-- Insert L-BHN My Services data into tmp table
INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_visits
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
  CAST(null as INT) AS one_time_payments_with_autopay_setup, --new as of 2017-11-22 (visits)
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
INSERT INTO TABLE ${env:TMP_db}.sbnet_exec_monthly_agg_src_visits
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
  size(collect_set(if(array_contains(message__feature__name,'Custom Event 36') AND state__view__current_page__page_type='SMB',visit__visit_id,NULL))) AS one_time_payment_attempts,
  size(collect_set(if(array_contains(message__feature__name,'Custom Event 31') AND state__view__current_page__page_type='SMB',visit__visit_id,NULL))) AS one_time_payments,
  CAST(null as INT) AS one_time_payments_with_autopay_setup, --new as of 2017-11-22 (visits)
  size(collect_set(if(array_contains(message__feature__name,'Custom Event 19') AND state__view__current_page__page_type='SMB',visit__visit_id,NULL))) AS auto_pay_setup_attempts,
  size(collect_set(if(array_contains(message__feature__name,'Custom Event 24') AND state__view__current_page__page_type='SMB',visit__visit_id,NULL))) AS auto_pay_setup_successes,
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
