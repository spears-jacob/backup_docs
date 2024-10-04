set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_counts_hourly_counts_households_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_counts_hourly_counts_households_columns AS
    SELECT
        'L-CHTR' as company,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccess' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE AND message__feature__feature_step_name IN('oneTimePaymentAppSuccess') AND message__feature__feature_step_changed = TRUE)) , visit__account__enc_account_number, Null))) AS one_time_payment_updated_households,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccessAutoPay' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE AND message__feature__feature_step_name IN('oneTimePaymentAutoPayAppSuccess') AND message__feature__feature_step_changed = TRUE)) , visit__account__enc_account_number, Null))) AS otp_with_autopay_successes_households,
        SIZE(COLLECT_SET(IF( message__name = 'Create Account Register', visit__account__enc_account_number, Null))) AS account_creation_attempts_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND message__name = 'Username Recovery Next' , visit__account__enc_account_number, Null))) AS username_recovery_attempts_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND message__name = 'Password Recovery Next' , visit__account__enc_account_number, Null))) AS reset_password_attempts_households,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND LOWER(message__name) IN ('overview') , visit__account__enc_account_number, Null))) AS auth_homepage_page_views_households,
        SIZE(COLLECT_SET(IF( UPPER(visit__user__role) = 'LOGGED IN', visit__account__enc_account_number, Null))) AS authenticated_visits_households,
        SIZE(COLLECT_SET(IF( state__view__current_page__page_name = 'pay-bill.autopay-enrollment-confirmation', visit__account__enc_account_number, Null))) AS auto_pay_setup_successes_households,
        SIZE(COLLECT_SET(IF( message__name = 'Confirm Cancel Appointment'  AND state__view__current_page__section = 'Appointment Tracking' , visit__account__enc_account_number, Null))) AS cancelled_service_appointments_households,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND LOWER(message__name) = 'contact us' , visit__account__enc_account_number, Null))) AS contact_us_page_views_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('account summary') , visit__account__enc_account_number, Null))) AS footer_manageaccount_account_summary_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('billing') , visit__account__enc_account_number, Null))) AS footer_manageaccount_billing_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('california privacy policy') , visit__account__enc_account_number, Null))) AS footer_legal_ca_privacy_rights_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('about us') , visit__account__enc_account_number, Null))) AS footer_charter_corporate_about_us_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('guarantee') , visit__account__enc_account_number, Null))) AS footer_charter_corporate_guarantee_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('contact us') , visit__account__enc_account_number, Null))) AS footer_contactus_contact_us_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('facebook') , visit__account__enc_account_number, Null))) AS footer_social_facebook_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('find a spectrum store') , visit__account__enc_account_number, Null))) AS footer_contactus_find_spectrum_store_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('give website feedback') , visit__account__enc_account_number, Null))) AS footer_contactus_give_website_feedback_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('linkedin') , visit__account__enc_account_number, Null))) AS footer_social_linkedin_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('manage users') , visit__account__enc_account_number, Null))) AS footer_manageaccount_manage_users_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('policies') , visit__account__enc_account_number, Null))) AS footer_legal_policies_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('your privacy rights') , visit__account__enc_account_number, Null))) AS footer_legal_privacy_rights_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('profile settings') , visit__account__enc_account_number, Null))) AS footer_manageaccount_settings_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('twitter') , visit__account__enc_account_number, Null))) AS footer_social_twitter_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('voice') , visit__account__enc_account_number, Null))) AS footer_manageaccount_voice_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('get weather outage info') , visit__account__enc_account_number, Null))) AS footer_contactus_weather_outage_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('youtube') , visit__account__enc_account_number, Null))) AS footer_social_youtube_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ( 'account summary tab switch' ) , visit__account__enc_account_number, Null))) AS localnav_acct_summary_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN( 'billing tab switch' ) , visit__account__enc_account_number, Null))) AS localnav_billing_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ( 'profile and settings tab switch' ) , visit__account__enc_account_number, Null))) AS localnav_settings_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) = 'users tab switch' , visit__account__enc_account_number, Null))) AS localnav_users_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ( 'voice tab switch' ) , visit__account__enc_account_number, Null))) AS localnav_voice_households,
        SIZE(COLLECT_SET(IF(  message__category = 'Custom Link'  AND UPPER(message__name) = 'SIGN IN SUBMIT' , visit__account__enc_account_number, Null))) AS login_attempts_adobe_households,
        SIZE(COLLECT_SET(IF(  message__category = 'Page View'  AND UPPER(state__view__current_page__page_name) = 'CREATE ACCOUNT SUMMARY' , visit__account__enc_account_number, Null))) AS new_admin_accounts_created_households,
        SIZE(COLLECT_SET(IF( message__name = 'Create Account Summary', visit__account__enc_account_number, Null))) AS new_accounts_created_households,
        SIZE(COLLECT_SET(IF(  message__category = 'Page View'  AND  UPPER(state__view__current_page__page_name) = 'MANAGE USERS PAGE'    AND UPPER(visit__application_details__referrer_link) = 'ADD NEW USER CONFIRM INFO NEXT'    AND  UPPER(state__view__current_page__page_name) = 'MANAGE USERS PAGE' , visit__account__enc_account_number, Null))) AS sub_acct_created_households,
        SIZE(COLLECT_SET(IF( visit__device__operating_system RLIKE '.*iOS|Android.*', visit__account__enc_account_number, Null))) AS os_ios_or_android_households,
        SIZE(COLLECT_SET(IF( visit__device__operating_system NOT RLIKE '.*iOS|Android.*', visit__account__enc_account_number, Null))) AS os_not_ios_or_android_households,
        SIZE(COLLECT_SET(IF(  state__view__current_page__section = 'Appointment Tracking'  AND message__name = 'Confirm Reschedule Appointment' , visit__account__enc_account_number, Null))) AS rescheduled_service_appointments_households,
        SIZE(COLLECT_SET(IF( state__search__text IS NOT NULL, visit__account__enc_account_number, Null))) AS search_action_households,
        SIZE(COLLECT_SET(IF(  message__category = 'Custom Link'   AND  visit__settings['post_prop12'] RLIKE '.*Search.*Results.*'   AND (message__category = 'Custom Link' OR UPPER(message__name) = 'SEARCH-VIEW-ALL' OR message__category = 'Custom Link' OR UPPER(message__name) RLIKE 'SEARCH-RESULT.*'  OR UPPER(message__name) = 'SUPPORT TOPICS') , visit__account__enc_account_number, Null))) AS search_results_clicked_households,
        SIZE(COLLECT_SET(IF( TRUE, visit__account__enc_account_number, Null))) AS site_unique_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND message__name = 'Add New User' , visit__account__enc_account_number, Null))) AS sub_user_creation_attempts_households,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'   AND  state__view__current_page__section = 'Support' , visit__account__enc_account_number, Null))) AS support_page_views_households,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND LOWER(message__name) IN ('login') , visit__account__enc_account_number, Null))) AS unauth_homepage_page_views_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('support') , visit__account__enc_account_number, Null))) AS utilitynav_support_households,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND lower(message__name) IN(LOWER('download statement'),LOWER('pay-bill.billing-statement-download')) , visit__account__enc_account_number, Null))) AS online_statement_views_households,
        'asp' AS platform,
        'sb' AS domain,
        prod.epoch_datehour(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_hour_denver,
        epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver
    FROM asp_v_sbnet_events
         
    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
    GROUP BY
        epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver'),
        prod.epoch_datehour(cast(message__timestamp * 1000 as bigint),'America/Denver'),
        'L-CHTR'
    ;
INSERT OVERWRITE TABLE prod.asp_counts_hourly
PARTITION(unit,platform,domain,company,date_denver,source_table)

    SELECT  value,
            metric,
            date_hour_denver,
            'households',
            'asp',
            'sb',
            company,
            date_denver,
            'asp_v_sbnet_events'
    FROM (SELECT  company,
                  date_denver,
                  date_hour_denver,
                  MAP(

                      'one_time_payment_updated|One Time Payment Successes|SpectrumBusiness.net_adobe||', one_time_payment_updated_households,
                      'otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|SpectrumBusiness.net_adobe||', otp_with_autopay_successes_households,
                      'account_creation_attempts|Account Creation Attempts|SpectrumBusiness.net_adobe||', account_creation_attempts_households,
                      'username_recovery_attempts|Attempts to Recover ID|SpectrumBusiness.net_adobe||', username_recovery_attempts_households,
                      'reset_password_attempts|Attempts to Reset Password|SpectrumBusiness.net_adobe||', reset_password_attempts_households,
                      'auth_homepage_page_views|Authenticated Homepage Page Views|SpectrumBusiness.net_adobe||', auth_homepage_page_views_households,
                      'authenticated_visits|Authenticated Visits|SpectrumBusiness.net_adobe||', authenticated_visits_households,
                      'auto_pay_setup_successes|Auto Pay Enrollment Successes|SpectrumBusiness.net_adobe||', auto_pay_setup_successes_households,
                      'cancelled_service_appointments|Cancelled Service Appointments|SpectrumBusiness.net_adobe||', cancelled_service_appointments_households,
                      'contact_us_page_views|Contact Us Page Views|SpectrumBusiness.net_adobe||', contact_us_page_views_households,
                      'footer_manageaccount_account_summary|Footer - Account Summary|SpectrumBusiness.net_adobe||', footer_manageaccount_account_summary_households,
                      'footer_manageaccount_billing|Footer - Billing|SpectrumBusiness.net_adobe||', footer_manageaccount_billing_households,
                      'footer_legal_ca_privacy_rights|Footer - CA Privacy Rights|SpectrumBusiness.net_adobe||', footer_legal_ca_privacy_rights_households,
                      'footer_charter_corporate_about_us|Footer - Charter Corporate About Us|SpectrumBusiness.net_adobe||', footer_charter_corporate_about_us_households,
                      'footer_charter_corporate_guarantee|Footer - Charter Corporate Guarantee|SpectrumBusiness.net_adobe||', footer_charter_corporate_guarantee_households,
                      'footer_contactus_contact_us|Footer - Contact Us|SpectrumBusiness.net_adobe||', footer_contactus_contact_us_households,
                      'footer_social_facebook|Footer - Facebook|SpectrumBusiness.net_adobe||', footer_social_facebook_households,
                      'footer_contactus_find_spectrum_store|Footer - Find a Spectrum Store|SpectrumBusiness.net_adobe||', footer_contactus_find_spectrum_store_households,
                      'footer_contactus_give_website_feedback|Footer - Give Website Feedback |SpectrumBusiness.net_adobe||', footer_contactus_give_website_feedback_households,
                      'footer_social_linkedin|Footer - LinkedIn|SpectrumBusiness.net_adobe||', footer_social_linkedin_households,
                      'footer_manageaccount_manage_users|Footer - Manage Users|SpectrumBusiness.net_adobe||', footer_manageaccount_manage_users_households,
                      'footer_legal_policies|Footer - Policies|SpectrumBusiness.net_adobe||', footer_legal_policies_households,
                      'footer_legal_privacy_rights|Footer - Privacy Rights|SpectrumBusiness.net_adobe||', footer_legal_privacy_rights_households,
                      'footer_manageaccount_settings|Footer - Settings|SpectrumBusiness.net_adobe||', footer_manageaccount_settings_households,
                      'footer_social_twitter|Footer - Twitter|SpectrumBusiness.net_adobe||', footer_social_twitter_households,
                      'footer_manageaccount_voice|Footer - Voice|SpectrumBusiness.net_adobe||', footer_manageaccount_voice_households,
                      'footer_contactus_weather_outage|Footer - Weather Outage|SpectrumBusiness.net_adobe||', footer_contactus_weather_outage_households,
                      'footer_social_youtube|Footer - Youtube|SpectrumBusiness.net_adobe||', footer_social_youtube_households,
                      'localnav_acct_summary|LocalNav - Account Summary|SpectrumBusiness.net_adobe||', localnav_acct_summary_households,
                      'localnav_billing|LocalNav - Billing|SpectrumBusiness.net_adobe||', localnav_billing_households,
                      'localnav_settings|LocalNav - Settings|SpectrumBusiness.net_adobe||', localnav_settings_households,
                      'localnav_users|LocalNav - Users|SpectrumBusiness.net_adobe||', localnav_users_households,
                      'localnav_voice|LocalNav - Voice|SpectrumBusiness.net_adobe||', localnav_voice_households,
                      'login_attempts_adobe|Login Attempts|SpectrumBusiness.net_adobe||', login_attempts_adobe_households,
                      'new_admin_accounts_created|New Admin Accounts Created|SpectrumBusiness.net_adobe||', new_admin_accounts_created_households,
                      'new_accounts_created|New IDs Created|SpectrumBusiness.net_adobe||', new_accounts_created_households,
                      'sub_acct_created|New Sub Accounts Created|SpectrumBusiness.net_adobe||', sub_acct_created_households,
                      'os_ios_or_android|Operating System - iOS or Android|SpectrumBusiness.net_adobe||', os_ios_or_android_households,
                      'os_not_ios_or_android|Operating System - Not iOS or Android|SpectrumBusiness.net_adobe||', os_not_ios_or_android_households,
                      'rescheduled_service_appointments|Rescheduled Service Appointments|SpectrumBusiness.net_adobe||', rescheduled_service_appointments_households,
                      'search_action|Search Action|SpectrumBusiness.net_adobe||', search_action_households,
                      'search_results_clicked|Search Results Clicked|SpectrumBusiness.net_adobe||', search_results_clicked_households,
                      'site_unique|Site Unique Values|SpectrumBusiness.net_adobe||', site_unique_households,
                      'sub_user_creation_attempts|Sub User Creation Attempts|SpectrumBusiness.net_adobe||', sub_user_creation_attempts_households,
                      'support_page_views|Support Section Page Views|SpectrumBusiness.net_adobe||', support_page_views_households,
                      'unauth_homepage_page_views|Unauthenticated Homepage Page Views|SpectrumBusiness.net_adobe||', unauth_homepage_page_views_households,
                      'utilitynav_support|Utility Navigation - Support|SpectrumBusiness.net_adobe||', utilitynav_support_households,
                      'online_statement_views|View Online Statement|SpectrumBusiness.net_adobe||', online_statement_views_households
                  ) as map_column
          FROM ${env:TMP_db}.asp_counts_hourly_counts_households_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
