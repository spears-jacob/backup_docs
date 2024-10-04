set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_counts_daily_counts_instances_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_counts_daily_counts_instances_columns AS
    SELECT
        'L-CHTR' as company,
        SUM(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccess' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE AND message__feature__feature_step_name IN('oneTimePaymentAppSuccess') AND message__feature__feature_step_changed = TRUE)) , 1, 0)) AS one_time_payment_updated,
        SUM(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccessAutoPay' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE AND message__feature__feature_step_name IN('oneTimePaymentAutoPayAppSuccess') AND message__feature__feature_step_changed = TRUE)) , 1, 0)) AS otp_with_autopay_successes,
        SUM(IF( message__name = 'Create Account Register', 1, 0)) AS account_creation_attempts,
        SUM(IF( message__category = 'Custom Link'  AND message__name = 'Username Recovery Next' , 1, 0)) AS username_recovery_attempts,
        SUM(IF( message__category = 'Custom Link'  AND message__name = 'Password Recovery Next' , 1, 0)) AS reset_password_attempts,
        SUM(IF( message__category = 'Page View'  AND LOWER(message__name) IN ('overview') , 1, 0)) AS auth_homepage_page_views,
        SUM(IF( UPPER(visit__user__role) = 'LOGGED IN', 1, 0)) AS authenticated_visits,
        SUM(IF( state__view__current_page__page_name = 'pay-bill.autopay-enrollment-confirmation', 1, 0)) AS auto_pay_setup_successes,
        SUM(IF( message__name = 'Confirm Cancel Appointment'  AND state__view__current_page__section = 'Appointment Tracking' , 1, 0)) AS cancelled_service_appointments,
        SUM(IF( message__category = 'Page View'  AND LOWER(message__name) = 'contact us' , 1, 0)) AS contact_us_page_views,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('account summary') , 1, 0)) AS footer_manageaccount_account_summary,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('billing') , 1, 0)) AS footer_manageaccount_billing,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('california privacy policy') , 1, 0)) AS footer_legal_ca_privacy_rights,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('about us') , 1, 0)) AS footer_charter_corporate_about_us,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('guarantee') , 1, 0)) AS footer_charter_corporate_guarantee,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('contact us') , 1, 0)) AS footer_contactus_contact_us,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('facebook') , 1, 0)) AS footer_social_facebook,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('find a spectrum store') , 1, 0)) AS footer_contactus_find_spectrum_store,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('give website feedback') , 1, 0)) AS footer_contactus_give_website_feedback,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('linkedin') , 1, 0)) AS footer_social_linkedin,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('manage users') , 1, 0)) AS footer_manageaccount_manage_users,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('policies') , 1, 0)) AS footer_legal_policies,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('your privacy rights') , 1, 0)) AS footer_legal_privacy_rights,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('profile settings') , 1, 0)) AS footer_manageaccount_settings,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('twitter') , 1, 0)) AS footer_social_twitter,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('voice') , 1, 0)) AS footer_manageaccount_voice,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('get weather outage info') , 1, 0)) AS footer_contactus_weather_outage,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('youtube') , 1, 0)) AS footer_social_youtube,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ( 'account summary tab switch' ) , 1, 0)) AS localnav_acct_summary,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN( 'billing tab switch' ) , 1, 0)) AS localnav_billing,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ( 'profile and settings tab switch' ) , 1, 0)) AS localnav_settings,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) = 'users tab switch' , 1, 0)) AS localnav_users,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ( 'voice tab switch' ) , 1, 0)) AS localnav_voice,
        SUM(IF(  message__category = 'Custom Link'  AND UPPER(message__name) = 'SIGN IN SUBMIT' , 1, 0)) AS login_attempts_adobe,
        SUM(IF(  message__category = 'Page View'  AND UPPER(state__view__current_page__page_name) = 'CREATE ACCOUNT SUMMARY' , 1, 0)) AS new_admin_accounts_created,
        SUM(IF( message__name = 'Create Account Summary', 1, 0)) AS new_accounts_created,
        SUM(IF(  message__category = 'Page View'  AND  UPPER(state__view__current_page__page_name) = 'MANAGE USERS PAGE'    AND UPPER(visit__application_details__referrer_link) = 'ADD NEW USER CONFIRM INFO NEXT'    AND  UPPER(state__view__current_page__page_name) = 'MANAGE USERS PAGE' , 1, 0)) AS sub_acct_created,
        SUM(IF( visit__device__operating_system RLIKE '.*iOS|Android.*', 1, 0)) AS os_ios_or_android,
        SUM(IF( visit__device__operating_system NOT RLIKE '.*iOS|Android.*', 1, 0)) AS os_not_ios_or_android,
        SUM(IF(  state__view__current_page__section = 'Appointment Tracking'  AND message__name = 'Confirm Reschedule Appointment' , 1, 0)) AS rescheduled_service_appointments,
        SUM(IF( state__search__text IS NOT NULL, 1, 0)) AS search_action,
        SUM(IF(  message__category = 'Custom Link'   AND  visit__settings['post_prop12'] RLIKE '.*Search.*Results.*'   AND (message__category = 'Custom Link' OR UPPER(message__name) = 'SEARCH-VIEW-ALL' OR message__category = 'Custom Link' OR UPPER(message__name) RLIKE 'SEARCH-RESULT.*'  OR UPPER(message__name) = 'SUPPORT TOPICS') , 1, 0)) AS search_results_clicked,
        SUM(IF( TRUE, 1, 0)) AS site_unique,
        SUM(IF( message__category = 'Custom Link'  AND message__name = 'Add New User' , 1, 0)) AS sub_user_creation_attempts,
        SUM(IF( message__category = 'Page View'   AND  state__view__current_page__section = 'Support' , 1, 0)) AS support_page_views,
        SUM(IF( message__category = 'Page View'  AND LOWER(message__name) IN ('login') , 1, 0)) AS unauth_homepage_page_views,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN('support') , 1, 0)) AS utilitynav_support,
        SUM(IF( message__category = 'Custom Link'  AND lower(message__name) IN(LOWER('download statement'),LOWER('pay-bill.billing-statement-download')) , 1, 0)) AS online_statement_views,
        'asp' AS platform,
        'sb' AS domain,
        
        epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver
    FROM asp_v_sbnet_events
         
    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
    GROUP BY
        epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver'),
        
        'L-CHTR'
    ;
INSERT OVERWRITE TABLE prod.asp_counts_daily
PARTITION(unit,platform,domain,company,date_denver,source_table)

    SELECT  value,
            metric,
            
            'instances',
            'asp',
            'sb',
            company,
            date_denver,
            'asp_v_sbnet_events'
    FROM (SELECT  company,
                  date_denver,
                  
                  MAP(

                      'one_time_payment_updated|One Time Payment Successes|SpectrumBusiness.net_adobe||', one_time_payment_updated,
                      'otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|SpectrumBusiness.net_adobe||', otp_with_autopay_successes,
                      'account_creation_attempts|Account Creation Attempts|SpectrumBusiness.net_adobe||', account_creation_attempts,
                      'username_recovery_attempts|Attempts to Recover ID|SpectrumBusiness.net_adobe||', username_recovery_attempts,
                      'reset_password_attempts|Attempts to Reset Password|SpectrumBusiness.net_adobe||', reset_password_attempts,
                      'auth_homepage_page_views|Authenticated Homepage Page Views|SpectrumBusiness.net_adobe||', auth_homepage_page_views,
                      'authenticated_visits|Authenticated Visits|SpectrumBusiness.net_adobe||', authenticated_visits,
                      'auto_pay_setup_successes|Auto Pay Enrollment Successes|SpectrumBusiness.net_adobe||', auto_pay_setup_successes,
                      'cancelled_service_appointments|Cancelled Service Appointments|SpectrumBusiness.net_adobe||', cancelled_service_appointments,
                      'contact_us_page_views|Contact Us Page Views|SpectrumBusiness.net_adobe||', contact_us_page_views,
                      'footer_manageaccount_account_summary|Footer - Account Summary|SpectrumBusiness.net_adobe||', footer_manageaccount_account_summary,
                      'footer_manageaccount_billing|Footer - Billing|SpectrumBusiness.net_adobe||', footer_manageaccount_billing,
                      'footer_legal_ca_privacy_rights|Footer - CA Privacy Rights|SpectrumBusiness.net_adobe||', footer_legal_ca_privacy_rights,
                      'footer_charter_corporate_about_us|Footer - Charter Corporate About Us|SpectrumBusiness.net_adobe||', footer_charter_corporate_about_us,
                      'footer_charter_corporate_guarantee|Footer - Charter Corporate Guarantee|SpectrumBusiness.net_adobe||', footer_charter_corporate_guarantee,
                      'footer_contactus_contact_us|Footer - Contact Us|SpectrumBusiness.net_adobe||', footer_contactus_contact_us,
                      'footer_social_facebook|Footer - Facebook|SpectrumBusiness.net_adobe||', footer_social_facebook,
                      'footer_contactus_find_spectrum_store|Footer - Find a Spectrum Store|SpectrumBusiness.net_adobe||', footer_contactus_find_spectrum_store,
                      'footer_contactus_give_website_feedback|Footer - Give Website Feedback |SpectrumBusiness.net_adobe||', footer_contactus_give_website_feedback,
                      'footer_social_linkedin|Footer - LinkedIn|SpectrumBusiness.net_adobe||', footer_social_linkedin,
                      'footer_manageaccount_manage_users|Footer - Manage Users|SpectrumBusiness.net_adobe||', footer_manageaccount_manage_users,
                      'footer_legal_policies|Footer - Policies|SpectrumBusiness.net_adobe||', footer_legal_policies,
                      'footer_legal_privacy_rights|Footer - Privacy Rights|SpectrumBusiness.net_adobe||', footer_legal_privacy_rights,
                      'footer_manageaccount_settings|Footer - Settings|SpectrumBusiness.net_adobe||', footer_manageaccount_settings,
                      'footer_social_twitter|Footer - Twitter|SpectrumBusiness.net_adobe||', footer_social_twitter,
                      'footer_manageaccount_voice|Footer - Voice|SpectrumBusiness.net_adobe||', footer_manageaccount_voice,
                      'footer_contactus_weather_outage|Footer - Weather Outage|SpectrumBusiness.net_adobe||', footer_contactus_weather_outage,
                      'footer_social_youtube|Footer - Youtube|SpectrumBusiness.net_adobe||', footer_social_youtube,
                      'localnav_acct_summary|LocalNav - Account Summary|SpectrumBusiness.net_adobe||', localnav_acct_summary,
                      'localnav_billing|LocalNav - Billing|SpectrumBusiness.net_adobe||', localnav_billing,
                      'localnav_settings|LocalNav - Settings|SpectrumBusiness.net_adobe||', localnav_settings,
                      'localnav_users|LocalNav - Users|SpectrumBusiness.net_adobe||', localnav_users,
                      'localnav_voice|LocalNav - Voice|SpectrumBusiness.net_adobe||', localnav_voice,
                      'login_attempts_adobe|Login Attempts|SpectrumBusiness.net_adobe||', login_attempts_adobe,
                      'new_admin_accounts_created|New Admin Accounts Created|SpectrumBusiness.net_adobe||', new_admin_accounts_created,
                      'new_accounts_created|New IDs Created|SpectrumBusiness.net_adobe||', new_accounts_created,
                      'sub_acct_created|New Sub Accounts Created|SpectrumBusiness.net_adobe||', sub_acct_created,
                      'os_ios_or_android|Operating System - iOS or Android|SpectrumBusiness.net_adobe||', os_ios_or_android,
                      'os_not_ios_or_android|Operating System - Not iOS or Android|SpectrumBusiness.net_adobe||', os_not_ios_or_android,
                      'rescheduled_service_appointments|Rescheduled Service Appointments|SpectrumBusiness.net_adobe||', rescheduled_service_appointments,
                      'search_action|Search Action|SpectrumBusiness.net_adobe||', search_action,
                      'search_results_clicked|Search Results Clicked|SpectrumBusiness.net_adobe||', search_results_clicked,
                      'site_unique|Site Unique Values|SpectrumBusiness.net_adobe||', site_unique,
                      'sub_user_creation_attempts|Sub User Creation Attempts|SpectrumBusiness.net_adobe||', sub_user_creation_attempts,
                      'support_page_views|Support Section Page Views|SpectrumBusiness.net_adobe||', support_page_views,
                      'unauth_homepage_page_views|Unauthenticated Homepage Page Views|SpectrumBusiness.net_adobe||', unauth_homepage_page_views,
                      'utilitynav_support|Utility Navigation - Support|SpectrumBusiness.net_adobe||', utilitynav_support,
                      'online_statement_views|View Online Statement|SpectrumBusiness.net_adobe||', online_statement_views
                  ) as map_column
          FROM ${env:TMP_db}.asp_counts_daily_counts_instances_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
