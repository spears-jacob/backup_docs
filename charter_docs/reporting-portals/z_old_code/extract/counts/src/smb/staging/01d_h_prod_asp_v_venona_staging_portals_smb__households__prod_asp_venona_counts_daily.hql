set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_venona_counts_daily_counts_households_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_venona_counts_daily_counts_households_columns AS
    SELECT
        COALESCE (visit__account__details__mso,'Unknown') as company,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND ((message__name = 'featureStop' AND message__feature__feature_step_name IN( 'apEnrollSuccess') AND message__feature__feature_step_changed = TRUE) OR ((message__event_case_id IN ('SPECNET_billPay_billPayStop_autopayEnrolled_triggeredByApplicationSuccess')))) , visit__account__enc_account_number, Null))) AS auto_pay_setup_successes_households,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'selectAction'   AND state__view__current_page__elements__standardized_name IN( 'downloadStatement', 'pay-bill.billing-statement-download') , visit__account__enc_account_number, Null))) AS view_online_statement_households,
        SIZE(COLLECT_SET(IF( FALSE, visit__account__enc_account_number, Null))) AS username_recovery_attempts_households,
        SIZE(COLLECT_SET(IF( FALSE, visit__account__enc_account_number, Null))) AS reset_password_attempts_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('homeAuth') , visit__account__enc_account_number, Null))) AS auth_homepage_page_views_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('cancelAppointmentSuccess') , visit__account__enc_account_number, Null))) AS cancelled_service_appointments_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND ((LOWER(message__name) = LOWER('featureStop')) OR (LOWER(message__name) = LOWER('pageView') AND LOWER(state__view__current_page__page_name) = LOWER('autoPayEnrollOtpSuccess'))) , visit__account__enc_account_number, Null))) AS combined_payment_successes_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('Contact Us - SMB') , visit__account__enc_account_number, Null))) AS contact_us_page_views_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND visit__device__operating_system RLIKE '.*iOS|Android.*' , visit__account__enc_account_number, Null))) AS site_unique_mobile_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND visit__device__operating_system NOT RLIKE '.*iOS|Android.*' , visit__account__enc_account_number, Null))) AS site_unique_pc_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Account Summary') , visit__account__enc_account_number, Null))) AS footer_manageaccount_account_summary_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Billing') , visit__account__enc_account_number, Null))) AS footer_manageaccount_billing_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('California Privacy Policy') , visit__account__enc_account_number, Null))) AS footer_legal_ca_privacy_rights_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('About Us') , visit__account__enc_account_number, Null))) AS footer_charter_corporate_about_us_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Guarantee') , visit__account__enc_account_number, Null))) AS footer_charter_corporate_guarantee_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Contact Us') , visit__account__enc_account_number, Null))) AS footer_contactus_contact_us_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Facebook') , visit__account__enc_account_number, Null))) AS footer_social_facebook_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Store Locations') , visit__account__enc_account_number, Null))) AS footer_contactus_find_spectrum_store_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Website Feedback') , visit__account__enc_account_number, Null))) AS footer_contactus_give_website_feedback_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('LinkedIn') , visit__account__enc_account_number, Null))) AS footer_social_linkedin_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Manage Users') , visit__account__enc_account_number, Null))) AS footer_manageaccount_manage_users_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Policies') , visit__account__enc_account_number, Null))) AS footer_legal_policies_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Your Privacy Rights') , visit__account__enc_account_number, Null))) AS footer_legal_privacy_rights_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Profile & Settings') , visit__account__enc_account_number, Null))) AS footer_manageaccount_settings_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Twitter') , visit__account__enc_account_number, Null))) AS footer_social_twitter_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Voice') , visit__account__enc_account_number, Null))) AS footer_manageaccount_voice_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Weather Outage') , visit__account__enc_account_number, Null))) AS footer_contactus_weather_outage_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('YouTube') , visit__account__enc_account_number, Null))) AS footer_social_youtube_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Account Summary') , visit__account__enc_account_number, Null))) AS localnav_acct_summary_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Billing') , visit__account__enc_account_number, Null))) AS localnav_billing_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Profile and Settings') , visit__account__enc_account_number, Null))) AS localnav_settings_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Users') , visit__account__enc_account_number, Null))) AS localnav_users_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Voice') , visit__account__enc_account_number, Null))) AS localnav_voice_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('loginStop') , visit__account__enc_account_number, Null))) AS login_attempts_adobe_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__api_name) = LOWER('sbNetMemberEdgeV2MembersCreate')   AND LOWER(application__api__response_code) RLIKE '2.*' , visit__account__enc_account_number, Null))) AS new_acct_created_all_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('applicationActivity')   AND LOWER(operation__operation_type) = LOWER('userAddSuccessBanner')   AND LOWER(message__context) = LOWER('Administrator') , visit__account__enc_account_number, Null))) AS new_admin_accounts_created_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('createUsername') , visit__account__enc_account_number, Null))) AS new_ids_created_attempts_households,
        SIZE(COLLECT_SET(IF( FALSE, visit__account__enc_account_number, Null))) AS new_ids_created_successes_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('applicationActivity')   AND LOWER(operation__operation_type) = LOWER('userAddSuccessBanner')   AND LOWER(message__context) = LOWER('Standard') , visit__account__enc_account_number, Null))) AS sub_acct_created_households,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccess' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE AND message__feature__feature_step_name IN('oneTimePaymentAppSuccess') AND message__feature__feature_step_changed = TRUE)) , visit__account__enc_account_number, Null))) AS one_time_payment_updated_households,
        SIZE(COLLECT_SET(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccessAutoPay' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE AND message__feature__feature_step_name IN('oneTimePaymentAutoPayAppSuccess') AND message__feature__feature_step_changed = TRUE)) , visit__account__enc_account_number, Null))) AS otp_with_autopay_successes_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView') , visit__account__enc_account_number, Null))) AS os_ios_or_android_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView') , visit__account__enc_account_number, Null))) AS os_not_ios_or_android_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('rescheduleAppointmentSuccess') , visit__account__enc_account_number, Null))) AS rescheduled_service_appointments_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND state__search__text IS NOT NULL , visit__account__enc_account_number, Null))) AS search_action_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView') , visit__account__enc_account_number, Null))) AS search_results_clicked_households,
        SIZE(COLLECT_SET(IF( TRUE, visit__account__enc_account_number, Null))) AS site_unique_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')   AND LOWER(message__name) = LOWER('userConfigSet')   AND visit__account__enc_account_number IS NOT NULL , visit__account__enc_account_number, Null))) AS site_unique_auth_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name)= LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__app_section) = LOWER('support') , visit__account__enc_account_number, Null))) AS support_page_views_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('homeUnauth') , visit__account__enc_account_number, Null))) AS unauth_homepage_page_views_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('header')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('support') , visit__account__enc_account_number, Null))) AS utilitynav_support_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(visit__device__device_type) RLIKE LOWER('and.*') , visit__account__enc_account_number, Null))) AS os_android_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(visit__device__device_type) RLIKE LOWER('ip.*') , visit__account__enc_account_number, Null))) AS os_ios_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(visit__device__device_type) NOT RLIKE LOWER('ip.*')   AND LOWER(visit__device__device_type) NOT RLIKE LOWER('and.*') , visit__account__enc_account_number, Null))) AS os_other_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name)= LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('recoveryEmailSent') , visit__account__enc_account_number, Null))) AS total_id_recovery_attempts_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('passwordRecoveryComplete') , visit__account__enc_account_number, Null))) AS successful_password_resets_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('resetAccountPassword') , visit__account__enc_account_number, Null))) AS password_reset_attempts_households,
        SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = 'smb'   AND message__name = 'featureStop'    AND operation__success = 'false'   AND message__feature__feature_step_name = 'oneTimePaymentStopFailure' , visit__account__enc_account_number, Null))) AS otp_with_autopay_failures_households,
        'asp' AS platform,
        'sb' AS domain,
        
        epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver
    FROM asp_v_venona_staging_portals_smb
         
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
            
            'households',
            'asp',
            'sb',
            company,
            date_denver,
            'asp_v_venona_staging_portals_smb'
    FROM (SELECT  company,
                  date_denver,
                  
                  MAP(

                      'auto_pay_setup_successes|Auto Pay Enrollment Successes|SpectrumBusiness.net||', auto_pay_setup_successes_households,
                      'view_online_statement|View Online Statement|SpectrumBusiness.net||', view_online_statement_households,
                      'username_recovery_attempts|Attempts to Recover ID|SpectrumBusiness.net||', username_recovery_attempts_households,
                      'reset_password_attempts|Attempts to Reset Password|SpectrumBusiness.net||', reset_password_attempts_households,
                      'auth_homepage_page_views|Authenticated Homepage Page Views|SpectrumBusiness.net||', auth_homepage_page_views_households,
                      'cancelled_service_appointments|Cancelled Service Appointments|SpectrumBusiness.net||', cancelled_service_appointments_households,
                      'combined_payment_successes|Combined One Time Payment and Auto Pay with One Time Payment Successes|SpectrumBusiness.net||', combined_payment_successes_households,
                      'contact_us_page_views|Contact Us Page Views|SpectrumBusiness.net||', contact_us_page_views_households,
                      'site_unique_mobile|Device Count Mobile|SpectrumBusiness.net||', site_unique_mobile_households,
                      'site_unique_pc|Device Count PC|SpectrumBusiness.net||', site_unique_pc_households,
                      'footer_manageaccount_account_summary|Footer - Account Summary|SpectrumBusiness.net||', footer_manageaccount_account_summary_households,
                      'footer_manageaccount_billing|Footer - Billing|SpectrumBusiness.net||', footer_manageaccount_billing_households,
                      'footer_legal_ca_privacy_rights|Footer - CA Privacy Rights|SpectrumBusiness.net||', footer_legal_ca_privacy_rights_households,
                      'footer_charter_corporate_about_us|Footer - Charter Corporate About Us|SpectrumBusiness.net||', footer_charter_corporate_about_us_households,
                      'footer_charter_corporate_guarantee|Footer - Charter Corporate Guarantee|SpectrumBusiness.net||', footer_charter_corporate_guarantee_households,
                      'footer_contactus_contact_us|Footer - Contact Us|SpectrumBusiness.net||', footer_contactus_contact_us_households,
                      'footer_social_facebook|Footer - Facebook|SpectrumBusiness.net||', footer_social_facebook_households,
                      'footer_contactus_find_spectrum_store|Footer - Find a Spectrum Store|SpectrumBusiness.net||', footer_contactus_find_spectrum_store_households,
                      'footer_contactus_give_website_feedback|Footer - Give Website Feedback |SpectrumBusiness.net||', footer_contactus_give_website_feedback_households,
                      'footer_social_linkedin|Footer - LinkedIn|SpectrumBusiness.net||', footer_social_linkedin_households,
                      'footer_manageaccount_manage_users|Footer - Manage Users|SpectrumBusiness.net||', footer_manageaccount_manage_users_households,
                      'footer_legal_policies|Footer - Policies|SpectrumBusiness.net||', footer_legal_policies_households,
                      'footer_legal_privacy_rights|Footer - Privacy Rights|SpectrumBusiness.net||', footer_legal_privacy_rights_households,
                      'footer_manageaccount_settings|Footer - Settings|SpectrumBusiness.net||', footer_manageaccount_settings_households,
                      'footer_social_twitter|Footer - Twitter|SpectrumBusiness.net||', footer_social_twitter_households,
                      'footer_manageaccount_voice|Footer - Voice|SpectrumBusiness.net||', footer_manageaccount_voice_households,
                      'footer_contactus_weather_outage|Footer - Weather Outage|SpectrumBusiness.net||', footer_contactus_weather_outage_households,
                      'footer_social_youtube|Footer - Youtube|SpectrumBusiness.net||', footer_social_youtube_households,
                      'localnav_acct_summary|LocalNav - Account Summary|SpectrumBusiness.net||', localnav_acct_summary_households,
                      'localnav_billing|LocalNav - Billing|SpectrumBusiness.net||', localnav_billing_households,
                      'localnav_settings|LocalNav - Settings|SpectrumBusiness.net||', localnav_settings_households,
                      'localnav_users|LocalNav - Users|SpectrumBusiness.net||', localnav_users_households,
                      'localnav_voice|LocalNav - Voice|SpectrumBusiness.net||', localnav_voice_households,
                      'login_attempts_adobe|Login Attempts|SpectrumBusiness.net||', login_attempts_adobe_households,
                      'new_acct_created_all|New Accounts Created - All|SpectrumBusiness.net||', new_acct_created_all_households,
                      'new_admin_accounts_created|New Admin Accounts Created|SpectrumBusiness.net||', new_admin_accounts_created_households,
                      'new_ids_created_attempts|New IDs Created Attempts|SpectrumBusiness.net||', new_ids_created_attempts_households,
                      'new_ids_created_successes|New IDs Created Successes|SpectrumBusiness.net||', new_ids_created_successes_households,
                      'sub_acct_created|New Sub Accounts Created|SpectrumBusiness.net||', sub_acct_created_households,
                      'one_time_payment_updated|One Time Payment Successes|SpectrumBusiness.net||', one_time_payment_updated_households,
                      'otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|SpectrumBusiness.net||', otp_with_autopay_successes_households,
                      'os_ios_or_android|Operating System - iOS or Android|SpectrumBusiness.net||', os_ios_or_android_households,
                      'os_not_ios_or_android|Operating System - Not iOS or Android|SpectrumBusiness.net||', os_not_ios_or_android_households,
                      'rescheduled_service_appointments|Rescheduled Service Appointments|SpectrumBusiness.net||', rescheduled_service_appointments_households,
                      'search_action|Search Action|SpectrumBusiness.net||', search_action_households,
                      'search_results_clicked|Search Results Clicked|SpectrumBusiness.net||', search_results_clicked_households,
                      'site_unique|Site Unique Values|SpectrumBusiness.net||', site_unique_households,
                      'site_unique_auth|Site Unique Values Authenticated|SpectrumBusiness.net||', site_unique_auth_households,
                      'support_page_views|Support Section Page Views|SpectrumBusiness.net||', support_page_views_households,
                      'unauth_homepage_page_views|Unauthenticated Homepage Page Views|SpectrumBusiness.net||', unauth_homepage_page_views_households,
                      'utilitynav_support|Utility Navigation - Support|SpectrumBusiness.net||', utilitynav_support_households,
                      'os_android|Operating System - Android|SpectrumBusiness.net||', os_android_households,
                      'os_ios|Operating System - iOS|SpectrumBusiness.net||', os_ios_households,
                      'os_other|Operating System - Not Android or iOS|SpectrumBusiness.net||', os_other_households,
                      'total_id_recovery_attempts|Total ID Recovery Attempts|SpectrumBusiness.net||', total_id_recovery_attempts_households,
                      'successful_password_resets|Successful Password Resets|SpectrumBusiness.net||', successful_password_resets_households,
                      'password_reset_attempts|Password Reset Attempts|SpectrumBusiness.net||', password_reset_attempts_households,
                      'otp_with_autopay_failures|One Time Payment with Auto Pay Enrollment Failures|SpectrumBusiness.net||', otp_with_autopay_failures_households
                  ) as map_column
          FROM ${env:TMP_db}.asp_venona_counts_daily_counts_households_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
