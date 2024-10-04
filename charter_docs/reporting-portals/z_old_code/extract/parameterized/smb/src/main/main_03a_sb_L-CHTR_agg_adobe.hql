set hive.exec.max.dynamic.partitions=20000
;
SET hive.exec.max.dynamic.partitions.pernode=7000
;


USE ${env:ENVIRONMENT};

-- Drop and rebuild charter sb temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.sb_chtr_${env:CADENCE};

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.sb_chtr_${env:CADENCE} AS

SELECT --Page Views
    SUM(IF(message__category = 'Page View',1,0)) - SUM(IF(message__category = 'Page View' AND state__view__current_page__page_name = 'Login' AND state__view__current_page__section = 'Login' ,1,0)) AS my_account_page_views,
    --Self-Help Page
    SIZE(COLLECT_SET(IF(message__name = 'Confirm Reschedule Appointment' and state__view__current_page__section = 'Appointment Tracking',visit__visit_id,NULL))) AS rescheduled_service_appointments,
    SIZE(COLLECT_SET(IF(message__name = 'Confirm Cancel Appointment' and state__view__current_page__section = 'Appointment Tracking',visit__visit_id,NULL))) AS cancelled_service_appointments,
    --Support
    SUM(IF(message__category = 'Page View' and state__view__current_page__section = 'Support',1,0)) AS support_page_views,
    --OBP
    SUM(IF(message__category = 'Custom Link' AND lower(message__name) in ('download statement','pay-bill.billing-statement-download'),1,0)) AS online_statement_views, --new AS of 2018-03-08 for CL55 changes (instances)
    SIZE(COLLECT_SET(IF(message__name = 'Complete Payment' ,visit__visit_id,NULL))) AS one_time_payment_attempts, --new AS of 2017-08-21 (visits)
    SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'pay-bill.onetime-confirmation', visit__visit_id, NULL))) AS one_time_payments, --new AS of 2017-11-22 (visits)
    SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'pay-bill.onetime-confirmation-with-autopay-enrollment', visit__visit_id, NULL))) AS one_time_payments_with_autopay_setup, --new AS of 2017-11-22 (visits)
    SIZE(COLLECT_SET(IF(message__name = 'Save Auto-Pay',visit__visit_id,NULL))) AS auto_pay_setup_attempts, --new AS of 2017-08-21 (visits)
    SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'pay-bill.autopay-enrollment-confirmation', visit__visit_id, NULL))) AS auto_pay_setup_successes, --new AS of 2017-11-22 (visits)
    --New Id Creation
    SIZE(COLLECT_SET(IF(message__name = 'Create Account Register',visit__visit_id,NULL))) AS total_account_creation_attempts,
    SIZE(COLLECT_SET(IF(message__name = 'Create Account Summary',visit__visit_id,NULL))) AS total_new_accounts_created,
    SIZE(COLLECT_SET(IF(message__name = 'Create Account Register' and visit__isp__isp LIKE 'charter%',visit__visit_id,NULL))) AS new_account_creation_attempts_on_chtr_network,
    SIZE(COLLECT_SET(IF(message__name = 'Create Account Summary' and visit__isp__isp LIKE 'charter%',visit__visit_id,NULL))) AS new_accounts_created_on_chtr_network,
    SIZE(COLLECT_SET(IF(message__name = 'Create Account Register',visit__visit_id,NULL))) - SIZE(COLLECT_SET(IF(message__name = 'Create Account Register' and visit__isp__isp LIKE 'charter%',visit__visit_id,NULL))) AS new_account_creation_attempts_off_chtr_network,
    SIZE(COLLECT_SET(IF(message__name = 'Create Account Summary',visit__visit_id,NULL))) - SIZE(COLLECT_SET(IF(message__name = 'Create Account Summary' and visit__isp__isp LIKE 'charter%',visit__visit_id,NULL))) AS new_accounts_created_off_chtr_network,
    --Sub-User Creation
    SUM(IF(message__name = 'Add New User' and message__category = 'Custom Link',1,0)) AS total_sub_user_creation_attempts,
    SUM(IF(state__view__current_page__page_name = 'Manage Users Page' AND visit__application_details__referrer_link = 'Add New User Confirm Info Next',1,0)) AS total_new_sub_users_created,
    SUM(IF(message__name = 'Add New User' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*',1,0)) AS sub_user_creation_attempts_on_chtr_network,
    SUM(IF(state__view__current_page__page_name = 'Manage Users Page' AND visit__application_details__referrer_link = 'Add New User Confirm Info Next' and visit__isp__isp RLIKE 'charter.*',1,0)) AS sub_users_created_on_chtr_network,
    SUM(IF(message__name = 'Add New User' and message__category = 'Custom Link',1,0)) - SUM(IF(message__name = 'Add New User' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*',1,0)) AS sub_user_creation_attempts_off_chtr_network,
    SUM(IF(state__view__current_page__page_name = 'Manage Users Page' AND visit__application_details__referrer_link = 'Add New User Confirm Info Next',1,0)) - SUM(IF(state__view__current_page__page_name = 'Manage Users Page' AND visit__application_details__referrer_link = 'Add New User Confirm Info Next' and visit__isp__isp RLIKE 'charter.*',1,0)) AS sub_users_created_off_chtr_network,
    --Username Recovery
    SIZE(COLLECT_SET(IF(message__name = 'Username Recovery Next' and message__category = 'Custom Link', visit__visit_id,NULL))) AS total_username_recovery_attempts,
    SIZE(COLLECT_SET(IF(message__name = 'Account Recovery Return to Login' and message__category = 'Custom Link', visit__visit_id,NULL))) AS total_username_recovery_successes,
    SIZE(COLLECT_SET(IF(message__name = 'Username Recovery Next' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS username_recovery_attempts_on_chtr_network,
    SIZE(COLLECT_SET(IF(message__name = 'Account Recovery Return to Login' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS username_recovery_successes_on_chtr_network,
    SIZE(COLLECT_SET(IF(message__name = 'Username Recovery Next' and message__category = 'Custom Link', visit__visit_id,NULL))) - SIZE(COLLECT_SET(IF(message__name = 'Username Recovery Next' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS username_recovery_attempts_off_chtr_network,
    SIZE(COLLECT_SET(IF(message__name = 'Account Recovery Return to Login' and message__category = 'Custom Link', visit__visit_id,NULL))) - SIZE(COLLECT_SET(IF(message__name = 'Account Recovery Return to Login' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS username_recovery_successes_off_chtr_network,
    --Password Reset
    SIZE(COLLECT_SET(IF(message__name = 'Password Recovery Next' and message__category = 'Custom Link', visit__visit_id, NULL))) AS total_attempts_to_reset_password,
    SIZE(COLLECT_SET(IF(message__name RLIKE 'Account Recovery Password Reset Complete.*' AND message__category = 'Custom Link', visit__visit_id, NULL))) AS total_successful_password_resets,
    SIZE(COLLECT_SET(IF(message__name = 'Password Recovery Next' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id, NULL))) AS attempts_to_reset_password_on_chtr_network,
    SIZE(COLLECT_SET(IF(message__name RLIKE 'Account Recovery Password Reset Complete.*' AND message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS successful_password_resets_on_chtr_network,
    SIZE(COLLECT_SET(IF(message__name = 'Password Recovery Next' and message__category = 'Custom Link', visit__visit_id, NULL))) - SIZE(COLLECT_SET(IF(message__name = 'Password Recovery Next' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id, NULL))) AS attempts_to_reset_password_off_chtr_network,
    SIZE(COLLECT_SET(IF(message__name RLIKE 'Account Recovery Password Reset Complete.*' AND message__category = 'Custom Link', visit__visit_id, NULL))) - SIZE(COLLECT_SET(IF(message__name RLIKE 'Account Recovery Password Reset Complete.*' AND message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) AS successful_password_resets_off_chtr_network,
    SUM(IF(UPPER(message__name) = 'SIGN IN SUBMIT' AND message__category = 'Custom Link',1,0)) AS login_attempts_adobe,
    SIZE(COLLECT_SET(IF(UPPER(visit__user__role) = 'LOGGED IN', visit__visit_id, NULL))) AS authenticated_visits,
    SUM(IF(UPPER(state__view__current_page__section) = 'SUPPORT' AND message__category = 'Page View', 1, 0)) AS support_pvs,
    --==----> VIEW STATEMENT
    --==----> Updated 20 Feb 2018 CL 5.3
    SUM(IF(lower(message__name)
           IN('download statement',                     -- Prior to CL 5.3 (before 2018-02-14)
              'pay-bill.billing-statement-download')   -- Current CL 5.3 (2018-02-14 forward)
           AND message__category = 'Custom Link',1,0)) AS view_statement,
    SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__page_name) = 'PAY-BILL.ONETIME-CONFIRMATION' AND message__category = 'Page View',visit__visit_id, NULL)))  AS one_time_payment_updated,
    SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__page_name) = 'PAY-BILL.ONETIME-CONFIRMATION-WITH-AUTOPAY-ENROLLMENT' AND message__category = 'Page View',visit__visit_id, NULL)))  AS one_time_payment_w_autopay_updated,
    SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__page_name) = 'PAY-BILL.AUTOPAY-ENROLLMENT-CONFIRMATION' AND message__category = 'Page View',visit__visit_id, NULL)))  AS auto_payment_confirm_updated,
    SIZE(COLLECT_SET(IF(UPPER(message__name) = 'CONFIRM RESCHEDULE APPOINTMENT' AND message__category = 'Custom Link',visit__visit_id, NULL))) AS reschedules,
    SIZE(COLLECT_SET(IF(UPPER(message__name) = 'CONFIRM CANCEL APPOINTMENT' AND message__category = 'Custom Link',visit__visit_id, NULL))) AS cancels,
    SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__page_name) = 'CREATE ACCOUNT SUMMARY' AND message__category = 'Page View',visit__visit_id, NULL))) AS new_admin_accounts_created,
    SUM(IF(UPPER(visit__application_details__referrer_link) = 'ADD NEW USER CONFIRM INFO NEXT'
      AND UPPER(state__view__current_page__page_name) = 'MANAGE USERS PAGE'
      AND message__category = 'Page View', 1, 0)) AS sub_acct_created,
    --DPrince updated 2018-02-02
    SUM(IF(UPPER(message__name) = 'SUPPORT TOPICS'
        AND visit__settings["post_prop12"] RLIKE '.*Search.*Results.*'
        AND message__category = 'Custom Link' , 1, 0 ))
        -- added 2018-01-18, pseudo device_type by way of OS
        + SUM(IF(UPPER(message__name) = 'SEARCH-VIEW-ALL'
          AND message__category = 'Custom Link' , 1, 0 ))
        + SUM(IF(UPPER(message__name) RLIKE 'SEARCH-RESULT.*'
          AND message__category = 'Custom Link' , 1, 0 ))
        -- added 2018-02-22 for 2018-02-14 going forward
      AS search_results_clicked,
  SIZE(COLLECT_SET(case when visit__device__operating_system     RLIKE '.*iOS|Android.*' THEN visit__visit_id ELSE NULL END)) AS count_os_is_iOSAndroid,
  SIZE(COLLECT_SET(case when visit__device__operating_system NOT RLIKE '.*iOS|Android.*' THEN visit__visit_id ELSE NULL END)) AS count_os_not_iOSAndroid,

  SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__page_name) = 'PAY-BILL.ONETIME-CONFIRMATION' AND message__category = 'Page View',visit__visit_id, NULL)))
  + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__page_name) = 'PAY-BILL.ONETIME-CONFIRMATION-WITH-AUTOPAY-ENROLLMENT' AND message__category = 'Page View',visit__visit_id, NULL))) AS one_time_payment_updated_wotap,
  --
  SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__page_name) = 'PAY-BILL.AUTOPAY-ENROLLMENT-CONFIRMATION' AND message__category = 'Page View',visit__visit_id, NULL)))
  + SIZE(COLLECT_SET(IF(UPPER(state__view__current_page__page_name) = 'PAY-BILL.ONETIME-CONFIRMATION-WITH-AUTOPAY-ENROLLMENT' AND message__category = 'Page View',visit__visit_id, NULL))) AS auto_payment_confirm_updated_wotap,
  SIZE(COLLECT_SET(IF(visit__visit_id IS NOT NULL, visit__visit_id, NULL))) AS visits,
  'L-CHTR' AS company,
  ${env:pf}
FROM  (SELECT ${env:ap} AS ${env:pf},
              message__category,
              message__name,
              state__view__current_page__page_name,
              state__view__current_page__section,
              visit__application_details__referrer_link,
              visit__isp__isp,
              visit__visit_id,
              visit__device__operating_system,
              visit__settings,
              visit__user__role
       FROM asp_v_sbnet_events
       LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
       WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
     ) dictionary
GROUP BY ${env:pf}
;


INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  ( SELECT 'my_account_page_views' AS metric,
               my_account_page_views AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'rescheduled_service_appointments' AS metric,
               rescheduled_service_appointments AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'cancelled_service_appointments' AS metric,
               cancelled_service_appointments AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'support_page_views' AS metric,
               support_page_views AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'online_statement_views' AS metric,
               online_statement_views AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'one_time_payment_attempts' AS metric,
               one_time_payment_attempts AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'one_time_payments' AS metric,
               one_time_payments AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'one_time_payments_with_autopay_setup' AS metric,
               one_time_payments_with_autopay_setup AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'auto_pay_setup_attempts' AS metric,
               auto_pay_setup_attempts AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'auto_pay_setup_successes' AS metric,
               auto_pay_setup_successes AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'total_account_creation_attempts' AS metric,
               total_account_creation_attempts AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'total_new_accounts_created' AS metric,
               total_new_accounts_created AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'new_account_creation_attempts_on_chtr_network' AS metric,
               new_account_creation_attempts_on_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'new_accounts_created_on_chtr_network' AS metric,
               new_accounts_created_on_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'new_account_creation_attempts_off_chtr_network' AS metric,
               new_account_creation_attempts_off_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'new_accounts_created_off_chtr_network' AS metric,
               new_accounts_created_off_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'total_sub_user_creation_attempts' AS metric,
               total_sub_user_creation_attempts AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'total_new_sub_users_created' AS metric,
               total_new_sub_users_created AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'sub_user_creation_attempts_on_chtr_network' AS metric,
               sub_user_creation_attempts_on_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'sub_users_created_on_chtr_network' AS metric,
               sub_users_created_on_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'sub_user_creation_attempts_off_chtr_network' AS metric,
               sub_user_creation_attempts_off_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'sub_users_created_off_chtr_network' AS metric,
               sub_users_created_off_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'total_username_recovery_attempts' AS metric,
               total_username_recovery_attempts AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'total_username_recovery_successes' AS metric,
               total_username_recovery_successes AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'username_recovery_attempts_on_chtr_network' AS metric,
               username_recovery_attempts_on_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'username_recovery_successes_on_chtr_network' AS metric,
               username_recovery_successes_on_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'username_recovery_attempts_off_chtr_network' AS metric,
               username_recovery_attempts_off_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'username_recovery_successes_off_chtr_network' AS metric,
               username_recovery_successes_off_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'total_attempts_to_reset_password' AS metric,
               total_attempts_to_reset_password AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'total_successful_password_resets' AS metric,
               total_successful_password_resets AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'attempts_to_reset_password_on_chtr_network' AS metric,
               attempts_to_reset_password_on_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'successful_password_resets_on_chtr_network' AS metric,
               successful_password_resets_on_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'attempts_to_reset_password_off_chtr_network' AS metric,
               attempts_to_reset_password_off_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'successful_password_resets_off_chtr_network' AS metric,
               successful_password_resets_off_chtr_network AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'login_attempts_adobe' AS metric,
               login_attempts_adobe AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'authenticated_visits' AS metric,
               authenticated_visits AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'support_pvs' AS metric,
               support_pvs AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'view_statement' AS metric,
               view_statement AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'one_time_payment_updated' AS metric,
               one_time_payment_updated AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'one_time_payment_w_autopay_updated' AS metric,
               one_time_payment_w_autopay_updated AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'auto_payment_confirm_updated' AS metric,
               auto_payment_confirm_updated AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'reschedules' AS metric,
               reschedules AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'cancels' AS metric,
               cancels AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'new_admin_accounts_created' AS metric,
               new_admin_accounts_created AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'sub_acct_created' AS metric,
               sub_acct_created AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'search_results_clicked' AS metric,
               search_results_clicked AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'count_os_is_iOSAndroid' AS metric,
               count_os_is_iOSAndroid AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT 'count_os_not_iOSAndroid' AS metric,
               count_os_not_iOSAndroid AS value,
               'Visits' AS unit,
               'sb' AS domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT  'one_time_payment_updated_wotap' as metric,
               one_time_payment_updated_wotap AS value,
               'Visits' as unit,
               'sb' as domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT  'auto_payment_confirm_updated_wotap' as metric,
               auto_payment_confirm_updated_wotap AS value,
               'Visits' as unit,
               'sb' as domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
        UNION
        SELECT  'visits' as metric,
               visits AS value,
               'Visits' as unit,
               'sb' as domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.sb_chtr_${env:CADENCE}
    ) q
;
