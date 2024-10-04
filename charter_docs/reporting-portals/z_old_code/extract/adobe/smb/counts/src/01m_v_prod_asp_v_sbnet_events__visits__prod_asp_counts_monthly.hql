set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_counts_monthly_counts_visits_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_counts_monthly_counts_visits_columns AS
    SELECT
        'L-CHTR' as company,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccess' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE AND message__feature__feature_step_name IN('oneTimePaymentAppSuccess') AND message__feature__feature_step_changed = TRUE)) , visit__visit_id, Null))) AS one_time_payment_updated_visits,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccessAutoPay' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE AND message__feature__feature_step_name IN('oneTimePaymentAutoPayAppSuccess') AND message__feature__feature_step_changed = TRUE)) , visit__visit_id, Null))) AS otp_with_autopay_successes_visits,
        SIZE(COLLECT_SET(IF( message__name = 'Create Account Register', visit__visit_id, Null))) AS account_creation_attempts_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND message__name = 'Username Recovery Next' , visit__visit_id, Null))) AS username_recovery_attempts_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND message__name = 'Password Recovery Next' , visit__visit_id, Null))) AS reset_password_attempts_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND LOWER(message__name) IN ('overview') , visit__visit_id, Null))) AS auth_homepage_page_views_visits,
        SIZE(COLLECT_SET(IF( UPPER(visit__user__role) = 'LOGGED IN', visit__visit_id, Null))) AS authenticated_visits_visits,
        SIZE(COLLECT_SET(IF( state__view__current_page__page_name = 'pay-bill.autopay-enrollment-confirmation', visit__visit_id, Null))) AS auto_pay_setup_successes_visits,
        SIZE(COLLECT_SET(IF( message__name = 'Confirm Cancel Appointment'  AND state__view__current_page__section = 'Appointment Tracking' , visit__visit_id, Null))) AS cancelled_service_appointments_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND LOWER(message__name) = 'contact us' , visit__visit_id, Null))) AS contact_us_page_views_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('account summary') , visit__visit_id, Null))) AS footer_manageaccount_account_summary_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('billing') , visit__visit_id, Null))) AS footer_manageaccount_billing_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('california privacy policy') , visit__visit_id, Null))) AS footer_legal_ca_privacy_rights_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('about us') , visit__visit_id, Null))) AS footer_charter_corporate_about_us_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('guarantee') , visit__visit_id, Null))) AS footer_charter_corporate_guarantee_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('contact us') , visit__visit_id, Null))) AS footer_contactus_contact_us_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('facebook') , visit__visit_id, Null))) AS footer_social_facebook_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('find a spectrum store') , visit__visit_id, Null))) AS footer_contactus_find_spectrum_store_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('give website feedback') , visit__visit_id, Null))) AS footer_contactus_give_website_feedback_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('linkedin') , visit__visit_id, Null))) AS footer_social_linkedin_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('manage users') , visit__visit_id, Null))) AS footer_manageaccount_manage_users_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('policies') , visit__visit_id, Null))) AS footer_legal_policies_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('your privacy rights') , visit__visit_id, Null))) AS footer_legal_privacy_rights_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('profile settings') , visit__visit_id, Null))) AS footer_manageaccount_settings_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('twitter') , visit__visit_id, Null))) AS footer_social_twitter_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('voice') , visit__visit_id, Null))) AS footer_manageaccount_voice_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('get weather outage info') , visit__visit_id, Null))) AS footer_contactus_weather_outage_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('youtube') , visit__visit_id, Null))) AS footer_social_youtube_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ( 'account summary tab switch' ) , visit__visit_id, Null))) AS localnav_acct_summary_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN( 'billing tab switch' ) , visit__visit_id, Null))) AS localnav_billing_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ( 'profile and settings tab switch' ) , visit__visit_id, Null))) AS localnav_settings_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) = 'users tab switch' , visit__visit_id, Null))) AS localnav_users_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ( 'voice tab switch' ) , visit__visit_id, Null))) AS localnav_voice_visits,
        SIZE(COLLECT_SET(IF(  message__category = 'Custom Link'  AND UPPER(message__name) = 'SIGN IN SUBMIT' , visit__visit_id, Null))) AS login_attempts_adobe_visits,
        SIZE(COLLECT_SET(IF(  message__category = 'Page View'  AND UPPER(state__view__current_page__page_name) = 'CREATE ACCOUNT SUMMARY' , visit__visit_id, Null))) AS new_admin_accounts_created_visits,
        SIZE(COLLECT_SET(IF( message__name = 'Create Account Summary', visit__visit_id, Null))) AS new_accounts_created_visits,
        SIZE(COLLECT_SET(IF(  message__category = 'Page View'  AND  UPPER(state__view__current_page__page_name) = 'MANAGE USERS PAGE'    AND UPPER(visit__application_details__referrer_link) = 'ADD NEW USER CONFIRM INFO NEXT'    AND  UPPER(state__view__current_page__page_name) = 'MANAGE USERS PAGE' , visit__visit_id, Null))) AS sub_acct_created_visits,
        SIZE(COLLECT_SET(IF( visit__device__operating_system RLIKE '.*iOS|Android.*', visit__visit_id, Null))) AS os_ios_or_android_visits,
        SIZE(COLLECT_SET(IF( visit__device__operating_system NOT RLIKE '.*iOS|Android.*', visit__visit_id, Null))) AS os_not_ios_or_android_visits,
        SIZE(COLLECT_SET(IF(  state__view__current_page__section = 'Appointment Tracking'  AND message__name = 'Confirm Reschedule Appointment' , visit__visit_id, Null))) AS rescheduled_service_appointments_visits,
        SIZE(COLLECT_SET(IF( state__search__text IS NOT NULL, visit__visit_id, Null))) AS search_action_visits,
        SIZE(COLLECT_SET(IF(  message__category = 'Custom Link'   AND  visit__settings['post_prop12'] RLIKE '.*Search.*Results.*'   AND (message__category = 'Custom Link' OR UPPER(message__name) = 'SEARCH-VIEW-ALL' OR message__category = 'Custom Link' OR UPPER(message__name) RLIKE 'SEARCH-RESULT.*'  OR UPPER(message__name) = 'SUPPORT TOPICS') , visit__visit_id, Null))) AS search_results_clicked_visits,
        SIZE(COLLECT_SET(IF( TRUE, visit__visit_id, Null))) AS site_unique_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND message__name = 'Add New User' , visit__visit_id, Null))) AS sub_user_creation_attempts_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'   AND  state__view__current_page__section = 'Support' , visit__visit_id, Null))) AS support_page_views_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND LOWER(message__name) IN ('login') , visit__visit_id, Null))) AS unauth_homepage_page_views_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('support') , visit__visit_id, Null))) AS utilitynav_support_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND lower(message__name) IN(LOWER('download statement'),LOWER('pay-bill.billing-statement-download')) , visit__visit_id, Null))) AS online_statement_views_visits,
        'asp' AS platform,
        'sb' AS domain,
        
        date_yearmonth(epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver')) as year_month_denver
    FROM asp_v_sbnet_events
         
    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
    GROUP BY
        date_yearmonth(epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver')),
        
        'L-CHTR'
    ;
INSERT OVERWRITE TABLE prod.asp_counts_monthly
PARTITION(unit,platform,domain,company,year_month_denver,source_table)

    SELECT  value,
            metric,
            
            'visits',
            'asp',
            'sb',
            company,
            year_month_denver,
            'asp_v_sbnet_events'
    FROM (SELECT  company,
                  year_month_denver,
                  
                  MAP(

                      'one_time_payment_updated|One Time Payment Successes|SpectrumBusiness.net_adobe||', one_time_payment_updated_visits,
                      'otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|SpectrumBusiness.net_adobe||', otp_with_autopay_successes_visits,
                      'account_creation_attempts|Account Creation Attempts|SpectrumBusiness.net_adobe||', account_creation_attempts_visits,
                      'username_recovery_attempts|Attempts to Recover ID|SpectrumBusiness.net_adobe||', username_recovery_attempts_visits,
                      'reset_password_attempts|Attempts to Reset Password|SpectrumBusiness.net_adobe||', reset_password_attempts_visits,
                      'auth_homepage_page_views|Authenticated Homepage Page Views|SpectrumBusiness.net_adobe||', auth_homepage_page_views_visits,
                      'authenticated_visits|Authenticated Visits|SpectrumBusiness.net_adobe||', authenticated_visits_visits,
                      'auto_pay_setup_successes|Auto Pay Enrollment Successes|SpectrumBusiness.net_adobe||', auto_pay_setup_successes_visits,
                      'cancelled_service_appointments|Cancelled Service Appointments|SpectrumBusiness.net_adobe||', cancelled_service_appointments_visits,
                      'contact_us_page_views|Contact Us Page Views|SpectrumBusiness.net_adobe||', contact_us_page_views_visits,
                      'footer_manageaccount_account_summary|Footer - Account Summary|SpectrumBusiness.net_adobe||', footer_manageaccount_account_summary_visits,
                      'footer_manageaccount_billing|Footer - Billing|SpectrumBusiness.net_adobe||', footer_manageaccount_billing_visits,
                      'footer_legal_ca_privacy_rights|Footer - CA Privacy Rights|SpectrumBusiness.net_adobe||', footer_legal_ca_privacy_rights_visits,
                      'footer_charter_corporate_about_us|Footer - Charter Corporate About Us|SpectrumBusiness.net_adobe||', footer_charter_corporate_about_us_visits,
                      'footer_charter_corporate_guarantee|Footer - Charter Corporate Guarantee|SpectrumBusiness.net_adobe||', footer_charter_corporate_guarantee_visits,
                      'footer_contactus_contact_us|Footer - Contact Us|SpectrumBusiness.net_adobe||', footer_contactus_contact_us_visits,
                      'footer_social_facebook|Footer - Facebook|SpectrumBusiness.net_adobe||', footer_social_facebook_visits,
                      'footer_contactus_find_spectrum_store|Footer - Find a Spectrum Store|SpectrumBusiness.net_adobe||', footer_contactus_find_spectrum_store_visits,
                      'footer_contactus_give_website_feedback|Footer - Give Website Feedback |SpectrumBusiness.net_adobe||', footer_contactus_give_website_feedback_visits,
                      'footer_social_linkedin|Footer - LinkedIn|SpectrumBusiness.net_adobe||', footer_social_linkedin_visits,
                      'footer_manageaccount_manage_users|Footer - Manage Users|SpectrumBusiness.net_adobe||', footer_manageaccount_manage_users_visits,
                      'footer_legal_policies|Footer - Policies|SpectrumBusiness.net_adobe||', footer_legal_policies_visits,
                      'footer_legal_privacy_rights|Footer - Privacy Rights|SpectrumBusiness.net_adobe||', footer_legal_privacy_rights_visits,
                      'footer_manageaccount_settings|Footer - Settings|SpectrumBusiness.net_adobe||', footer_manageaccount_settings_visits,
                      'footer_social_twitter|Footer - Twitter|SpectrumBusiness.net_adobe||', footer_social_twitter_visits,
                      'footer_manageaccount_voice|Footer - Voice|SpectrumBusiness.net_adobe||', footer_manageaccount_voice_visits,
                      'footer_contactus_weather_outage|Footer - Weather Outage|SpectrumBusiness.net_adobe||', footer_contactus_weather_outage_visits,
                      'footer_social_youtube|Footer - Youtube|SpectrumBusiness.net_adobe||', footer_social_youtube_visits,
                      'localnav_acct_summary|LocalNav - Account Summary|SpectrumBusiness.net_adobe||', localnav_acct_summary_visits,
                      'localnav_billing|LocalNav - Billing|SpectrumBusiness.net_adobe||', localnav_billing_visits,
                      'localnav_settings|LocalNav - Settings|SpectrumBusiness.net_adobe||', localnav_settings_visits,
                      'localnav_users|LocalNav - Users|SpectrumBusiness.net_adobe||', localnav_users_visits,
                      'localnav_voice|LocalNav - Voice|SpectrumBusiness.net_adobe||', localnav_voice_visits,
                      'login_attempts_adobe|Login Attempts|SpectrumBusiness.net_adobe||', login_attempts_adobe_visits,
                      'new_admin_accounts_created|New Admin Accounts Created|SpectrumBusiness.net_adobe||', new_admin_accounts_created_visits,
                      'new_accounts_created|New IDs Created|SpectrumBusiness.net_adobe||', new_accounts_created_visits,
                      'sub_acct_created|New Sub Accounts Created|SpectrumBusiness.net_adobe||', sub_acct_created_visits,
                      'os_ios_or_android|Operating System - iOS or Android|SpectrumBusiness.net_adobe||', os_ios_or_android_visits,
                      'os_not_ios_or_android|Operating System - Not iOS or Android|SpectrumBusiness.net_adobe||', os_not_ios_or_android_visits,
                      'rescheduled_service_appointments|Rescheduled Service Appointments|SpectrumBusiness.net_adobe||', rescheduled_service_appointments_visits,
                      'search_action|Search Action|SpectrumBusiness.net_adobe||', search_action_visits,
                      'search_results_clicked|Search Results Clicked|SpectrumBusiness.net_adobe||', search_results_clicked_visits,
                      'site_unique|Site Unique Values|SpectrumBusiness.net_adobe||', site_unique_visits,
                      'sub_user_creation_attempts|Sub User Creation Attempts|SpectrumBusiness.net_adobe||', sub_user_creation_attempts_visits,
                      'support_page_views|Support Section Page Views|SpectrumBusiness.net_adobe||', support_page_views_visits,
                      'unauth_homepage_page_views|Unauthenticated Homepage Page Views|SpectrumBusiness.net_adobe||', unauth_homepage_page_views_visits,
                      'utilitynav_support|Utility Navigation - Support|SpectrumBusiness.net_adobe||', utilitynav_support_visits,
                      'online_statement_views|View Online Statement|SpectrumBusiness.net_adobe||', online_statement_views_visits
                  ) as map_column
          FROM ${env:TMP_db}.asp_counts_monthly_counts_visits_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
