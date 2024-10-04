set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_venona_counts_daily_counts_devices_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_venona_counts_daily_counts_devices_columns AS
    SELECT
        COALESCE (visit__account__details__mso,'Unknown') as company,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND ((message__name = 'featureStop' AND message__feature__feature_name IN ('autoPayEnroll', 'autoPayEnrollment') AND operation__success = TRUE) OR ((message__event_case_id IN('SPECNET_billPay_billPayStop_autopayEnrolled_triggeredByApplicationSuccess', 'SPECNET_billPay_billPayStop_autopayEnrollmentWithPayment_triggeredByApplicationSuccess')))) , visit__device__enc_uuid, Null))) AS auto_pay_setup_successes_devices,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'selectAction'   AND state__view__current_page__elements__standardized_name IN( 'downloadStatement', 'pay-bill.billing-statement-download') , visit__device__enc_uuid, Null))) AS view_online_statement_devices,
        SIZE(COLLECT_SET(IF( FALSE, visit__device__enc_uuid, Null))) AS username_recovery_attempts_devices,
        SIZE(COLLECT_SET(IF( FALSE, visit__device__enc_uuid, Null))) AS reset_password_attempts_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('homeAuth') , visit__device__enc_uuid, Null))) AS auth_homepage_page_views_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('cancelAppointmentSuccess') , visit__device__enc_uuid, Null))) AS cancelled_service_appointments_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND ((LOWER(message__name) = LOWER('featureStop')) OR (LOWER(message__name) = LOWER('pageView') AND LOWER(state__view__current_page__page_name) = LOWER('autoPayEnrollOtpSuccess'))) , visit__device__enc_uuid, Null))) AS combined_payment_successes_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('Contact Us - SMB') , visit__device__enc_uuid, Null))) AS contact_us_page_views_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND visit__device__operating_system RLIKE '.*iOS|Android.*' , visit__device__enc_uuid, Null))) AS site_unique_mobile_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND visit__device__operating_system NOT RLIKE '.*iOS|Android.*' , visit__device__enc_uuid, Null))) AS site_unique_pc_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Account Summary') , visit__device__enc_uuid, Null))) AS footer_manageaccount_account_summary_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Billing') , visit__device__enc_uuid, Null))) AS footer_manageaccount_billing_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('California Privacy Policy') , visit__device__enc_uuid, Null))) AS footer_legal_ca_privacy_rights_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('About Us') , visit__device__enc_uuid, Null))) AS footer_charter_corporate_about_us_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Guarantee') , visit__device__enc_uuid, Null))) AS footer_charter_corporate_guarantee_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Contact Us') , visit__device__enc_uuid, Null))) AS footer_contactus_contact_us_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Facebook') , visit__device__enc_uuid, Null))) AS footer_social_facebook_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Store Locations') , visit__device__enc_uuid, Null))) AS footer_contactus_find_spectrum_store_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Website Feedback') , visit__device__enc_uuid, Null))) AS footer_contactus_give_website_feedback_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('LinkedIn') , visit__device__enc_uuid, Null))) AS footer_social_linkedin_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Manage Users') , visit__device__enc_uuid, Null))) AS footer_manageaccount_manage_users_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Policies') , visit__device__enc_uuid, Null))) AS footer_legal_policies_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Your Privacy Rights') , visit__device__enc_uuid, Null))) AS footer_legal_privacy_rights_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Profile & Settings') , visit__device__enc_uuid, Null))) AS footer_manageaccount_settings_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Twitter') , visit__device__enc_uuid, Null))) AS footer_social_twitter_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Voice') , visit__device__enc_uuid, Null))) AS footer_manageaccount_voice_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Weather Outage') , visit__device__enc_uuid, Null))) AS footer_contactus_weather_outage_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('YouTube') , visit__device__enc_uuid, Null))) AS footer_social_youtube_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Account Summary') , visit__device__enc_uuid, Null))) AS localnav_acct_summary_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Billing') , visit__device__enc_uuid, Null))) AS localnav_billing_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Profile and Settings') , visit__device__enc_uuid, Null))) AS localnav_settings_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Users') , visit__device__enc_uuid, Null))) AS localnav_users_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Voice') , visit__device__enc_uuid, Null))) AS localnav_voice_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('loginStop') , visit__device__enc_uuid, Null))) AS login_attempts_adobe_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__api_name) = LOWER('sbNetMemberEdgeV2MembersCreate')   AND LOWER(application__api__response_code) RLIKE '2.*' , visit__device__enc_uuid, Null))) AS new_acct_created_all_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('applicationActivity')   AND LOWER(operation__operation_type) = LOWER('userAddSuccessBanner')   AND LOWER(message__context) = LOWER('Administrator') , visit__device__enc_uuid, Null))) AS new_admin_accounts_created_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('createUsername') , visit__device__enc_uuid, Null))) AS new_ids_created_attempts_devices,
        SIZE(COLLECT_SET(IF( FALSE, visit__device__enc_uuid, Null))) AS new_ids_created_successes_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('applicationActivity')   AND LOWER(operation__operation_type) = LOWER('userAddSuccessBanner')   AND LOWER(message__context) = LOWER('Standard') , visit__device__enc_uuid, Null))) AS sub_acct_created_devices,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccess', 'otpSuccessAutoPay' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name IN ('oneTimeBillPayFlow', 'oneTimePayment') AND operation__success = TRUE AND ((message__feature__feature_step_changed = FALSE AND message__event_case_id <> 'SPECNET_selectAction_billPayStop_otp_exitBillPayFlow') OR (message__feature__feature_step_name IN('oneTimePaymentAppSuccess','oneTimePaymentAutoPayAppSuccess', 'paymentWithAutoPaySuccess', 'paymentSuccess') AND message__feature__feature_step_changed = TRUE)))) , visit__device__enc_uuid, Null))) AS one_time_payment_updated_devices,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccessAutoPay' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name IN ('oneTimeBillPayFlow', 'oneTimePayment') AND operation__success = TRUE AND message__feature__feature_step_name IN('oneTimePaymentAutoPayAppSuccess',  'paymentWithAutoPaySuccess') AND message__feature__feature_step_changed = TRUE)) , visit__device__enc_uuid, Null))) AS otp_with_autopay_successes_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView') , visit__device__enc_uuid, Null))) AS os_ios_or_android_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView') , visit__device__enc_uuid, Null))) AS os_not_ios_or_android_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('rescheduleAppointmentSuccess') , visit__device__enc_uuid, Null))) AS rescheduled_service_appointments_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND state__search__text IS NOT NULL , visit__device__enc_uuid, Null))) AS search_action_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView') , visit__device__enc_uuid, Null))) AS search_results_clicked_devices,
        SIZE(COLLECT_SET(IF( TRUE, visit__device__enc_uuid, Null))) AS site_unique_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')   AND LOWER(message__name) = LOWER('userConfigSet')   AND visit__account__enc_account_number IS NOT NULL , visit__device__enc_uuid, Null))) AS site_unique_auth_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name)= LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__app_section) = LOWER('support') , visit__device__enc_uuid, Null))) AS support_page_views_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('homeUnauth') , visit__device__enc_uuid, Null))) AS unauth_homepage_page_views_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('header')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('support') , visit__device__enc_uuid, Null))) AS utilitynav_support_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(visit__device__device_type) RLIKE LOWER('and.*') , visit__device__enc_uuid, Null))) AS os_android_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(visit__device__device_type) RLIKE LOWER('ip.*') , visit__device__enc_uuid, Null))) AS os_ios_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(visit__device__device_type) NOT RLIKE LOWER('ip.*')   AND LOWER(visit__device__device_type) NOT RLIKE LOWER('and.*') , visit__device__enc_uuid, Null))) AS os_other_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name)= LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('recoveryEmailSent') , visit__device__enc_uuid, Null))) AS total_id_recovery_attempts_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('passwordRecoveryComplete') , visit__device__enc_uuid, Null))) AS successful_password_resets_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('resetAccountPassword') , visit__device__enc_uuid, Null))) AS password_reset_attempts_devices,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = 'smb'   AND message__name = 'featureStop'    AND operation__success = 'false'   AND message__feature__feature_step_name = 'oneTimePaymentStopFailure' , visit__device__enc_uuid, Null))) AS otp_with_autopay_failures_devices,
        'asp' AS platform,
        'sb' AS domain,
        
        epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver
    FROM asp_v_venona_events_portals_smb
         
    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
    GROUP BY
        epoch_converter(cast(received__timestamp as bigint),'America/Denver'),
        
        COALESCE (visit__account__details__mso,'Unknown')
    ;
INSERT OVERWRITE TABLE prod.asp_venona_counts_daily
PARTITION(unit,platform,domain,company,date_denver,source_table)

    SELECT  value,
            metric,
            
            'devices',
            'asp',
            'sb',
            company,
            date_denver,
            'asp_v_venona_events_portals_smb'
    FROM (SELECT  company,
                  date_denver,
                  
                  MAP(

                      'auto_pay_setup_successes|Auto Pay Enrollment Successes|SpectrumBusiness.net||', auto_pay_setup_successes_devices,
                      'view_online_statement|View Online Statement|SpectrumBusiness.net||', view_online_statement_devices,
                      'username_recovery_attempts|Attempts to Recover ID|SpectrumBusiness.net||', username_recovery_attempts_devices,
                      'reset_password_attempts|Attempts to Reset Password|SpectrumBusiness.net||', reset_password_attempts_devices,
                      'auth_homepage_page_views|Authenticated Homepage Page Views|SpectrumBusiness.net||', auth_homepage_page_views_devices,
                      'cancelled_service_appointments|Cancelled Service Appointments|SpectrumBusiness.net||', cancelled_service_appointments_devices,
                      'combined_payment_successes|Combined One Time Payment and Auto Pay with One Time Payment Successes|SpectrumBusiness.net||', combined_payment_successes_devices,
                      'contact_us_page_views|Contact Us Page Views|SpectrumBusiness.net||', contact_us_page_views_devices,
                      'site_unique_mobile|Device Count Mobile|SpectrumBusiness.net||', site_unique_mobile_devices,
                      'site_unique_pc|Device Count PC|SpectrumBusiness.net||', site_unique_pc_devices,
                      'footer_manageaccount_account_summary|Footer - Account Summary|SpectrumBusiness.net||', footer_manageaccount_account_summary_devices,
                      'footer_manageaccount_billing|Footer - Billing|SpectrumBusiness.net||', footer_manageaccount_billing_devices,
                      'footer_legal_ca_privacy_rights|Footer - CA Privacy Rights|SpectrumBusiness.net||', footer_legal_ca_privacy_rights_devices,
                      'footer_charter_corporate_about_us|Footer - Charter Corporate About Us|SpectrumBusiness.net||', footer_charter_corporate_about_us_devices,
                      'footer_charter_corporate_guarantee|Footer - Charter Corporate Guarantee|SpectrumBusiness.net||', footer_charter_corporate_guarantee_devices,
                      'footer_contactus_contact_us|Footer - Contact Us|SpectrumBusiness.net||', footer_contactus_contact_us_devices,
                      'footer_social_facebook|Footer - Facebook|SpectrumBusiness.net||', footer_social_facebook_devices,
                      'footer_contactus_find_spectrum_store|Footer - Find a Spectrum Store|SpectrumBusiness.net||', footer_contactus_find_spectrum_store_devices,
                      'footer_contactus_give_website_feedback|Footer - Give Website Feedback |SpectrumBusiness.net||', footer_contactus_give_website_feedback_devices,
                      'footer_social_linkedin|Footer - LinkedIn|SpectrumBusiness.net||', footer_social_linkedin_devices,
                      'footer_manageaccount_manage_users|Footer - Manage Users|SpectrumBusiness.net||', footer_manageaccount_manage_users_devices,
                      'footer_legal_policies|Footer - Policies|SpectrumBusiness.net||', footer_legal_policies_devices,
                      'footer_legal_privacy_rights|Footer - Privacy Rights|SpectrumBusiness.net||', footer_legal_privacy_rights_devices,
                      'footer_manageaccount_settings|Footer - Settings|SpectrumBusiness.net||', footer_manageaccount_settings_devices,
                      'footer_social_twitter|Footer - Twitter|SpectrumBusiness.net||', footer_social_twitter_devices,
                      'footer_manageaccount_voice|Footer - Voice|SpectrumBusiness.net||', footer_manageaccount_voice_devices,
                      'footer_contactus_weather_outage|Footer - Weather Outage|SpectrumBusiness.net||', footer_contactus_weather_outage_devices,
                      'footer_social_youtube|Footer - Youtube|SpectrumBusiness.net||', footer_social_youtube_devices,
                      'localnav_acct_summary|LocalNav - Account Summary|SpectrumBusiness.net||', localnav_acct_summary_devices,
                      'localnav_billing|LocalNav - Billing|SpectrumBusiness.net||', localnav_billing_devices,
                      'localnav_settings|LocalNav - Settings|SpectrumBusiness.net||', localnav_settings_devices,
                      'localnav_users|LocalNav - Users|SpectrumBusiness.net||', localnav_users_devices,
                      'localnav_voice|LocalNav - Voice|SpectrumBusiness.net||', localnav_voice_devices,
                      'login_attempts_adobe|Login Attempts|SpectrumBusiness.net||', login_attempts_adobe_devices,
                      'new_acct_created_all|New Accounts Created - All|SpectrumBusiness.net||', new_acct_created_all_devices,
                      'new_admin_accounts_created|New Admin Accounts Created|SpectrumBusiness.net||', new_admin_accounts_created_devices,
                      'new_ids_created_attempts|New IDs Created Attempts|SpectrumBusiness.net||', new_ids_created_attempts_devices,
                      'new_ids_created_successes|New IDs Created Successes|SpectrumBusiness.net||', new_ids_created_successes_devices,
                      'sub_acct_created|New Sub Accounts Created|SpectrumBusiness.net||', sub_acct_created_devices,
                      'one_time_payment_updated|One Time Payment Successes|SpectrumBusiness.net||', one_time_payment_updated_devices,
                      'otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|SpectrumBusiness.net||', otp_with_autopay_successes_devices,
                      'os_ios_or_android|Operating System - iOS or Android|SpectrumBusiness.net||', os_ios_or_android_devices,
                      'os_not_ios_or_android|Operating System - Not iOS or Android|SpectrumBusiness.net||', os_not_ios_or_android_devices,
                      'rescheduled_service_appointments|Rescheduled Service Appointments|SpectrumBusiness.net||', rescheduled_service_appointments_devices,
                      'search_action|Search Action|SpectrumBusiness.net||', search_action_devices,
                      'search_results_clicked|Search Results Clicked|SpectrumBusiness.net||', search_results_clicked_devices,
                      'site_unique|Site Unique Values|SpectrumBusiness.net||', site_unique_devices,
                      'site_unique_auth|Site Unique Values Authenticated|SpectrumBusiness.net||', site_unique_auth_devices,
                      'support_page_views|Support Section Page Views|SpectrumBusiness.net||', support_page_views_devices,
                      'unauth_homepage_page_views|Unauthenticated Homepage Page Views|SpectrumBusiness.net||', unauth_homepage_page_views_devices,
                      'utilitynav_support|Utility Navigation - Support|SpectrumBusiness.net||', utilitynav_support_devices,
                      'os_android|Operating System - Android|SpectrumBusiness.net||', os_android_devices,
                      'os_ios|Operating System - iOS|SpectrumBusiness.net||', os_ios_devices,
                      'os_other|Operating System - Not Android or iOS|SpectrumBusiness.net||', os_other_devices,
                      'total_id_recovery_attempts|Total ID Recovery Attempts|SpectrumBusiness.net||', total_id_recovery_attempts_devices,
                      'successful_password_resets|Successful Password Resets|SpectrumBusiness.net||', successful_password_resets_devices,
                      'password_reset_attempts|Password Reset Attempts|SpectrumBusiness.net||', password_reset_attempts_devices,
                      'otp_with_autopay_failures|One Time Payment with Auto Pay Enrollment Failures|SpectrumBusiness.net||', otp_with_autopay_failures_devices
                  ) as map_column
          FROM ${env:TMP_db}.asp_venona_counts_daily_counts_devices_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
