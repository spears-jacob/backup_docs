set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_venona_counts_weekly_counts_instances_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_venona_counts_weekly_counts_instances_columns AS
    SELECT
        COALESCE (visit__account__details__mso,'Unknown') as company,
        SUM(IF( visit__application_details__application_name = 'SMB'  AND ((message__name = 'featureStop' AND message__feature__feature_step_name IN( 'apEnrollSuccess') AND message__feature__feature_step_changed = TRUE) OR ((message__event_case_id IN ('SPECNET_billPay_billPayStop_autopayEnrolled_triggeredByApplicationSuccess')))) , 1, 0)) AS auto_pay_setup_successes,
        SUM(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'selectAction'   AND state__view__current_page__elements__standardized_name IN( 'downloadStatement', 'pay-bill.billing-statement-download') , 1, 0)) AS view_online_statement,
        SUM(IF( FALSE, 1, 0)) AS username_recovery_attempts,
        SUM(IF( FALSE, 1, 0)) AS reset_password_attempts,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('homeAuth') , 1, 0)) AS auth_homepage_page_views,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('cancelAppointmentSuccess') , 1, 0)) AS cancelled_service_appointments,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND ((LOWER(message__name) = LOWER('featureStop')) OR (LOWER(message__name) = LOWER('pageView') AND LOWER(state__view__current_page__page_name) = LOWER('autoPayEnrollOtpSuccess'))) , 1, 0)) AS combined_payment_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('Contact Us - SMB') , 1, 0)) AS contact_us_page_views,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND visit__device__operating_system RLIKE '.*iOS|Android.*' , 1, 0)) AS site_unique_mobile,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND visit__device__operating_system NOT RLIKE '.*iOS|Android.*' , 1, 0)) AS site_unique_pc,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Account Summary') , 1, 0)) AS footer_manageaccount_account_summary,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Billing') , 1, 0)) AS footer_manageaccount_billing,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('California Privacy Policy') , 1, 0)) AS footer_legal_ca_privacy_rights,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('About Us') , 1, 0)) AS footer_charter_corporate_about_us,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Guarantee') , 1, 0)) AS footer_charter_corporate_guarantee,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Contact Us') , 1, 0)) AS footer_contactus_contact_us,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Facebook') , 1, 0)) AS footer_social_facebook,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Store Locations') , 1, 0)) AS footer_contactus_find_spectrum_store,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Website Feedback') , 1, 0)) AS footer_contactus_give_website_feedback,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('LinkedIn') , 1, 0)) AS footer_social_linkedin,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Manage Users') , 1, 0)) AS footer_manageaccount_manage_users,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Policies') , 1, 0)) AS footer_legal_policies,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Your Privacy Rights') , 1, 0)) AS footer_legal_privacy_rights,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Profile & Settings') , 1, 0)) AS footer_manageaccount_settings,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Twitter') , 1, 0)) AS footer_social_twitter,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Voice') , 1, 0)) AS footer_manageaccount_voice,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Weather Outage') , 1, 0)) AS footer_contactus_weather_outage,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('footer')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('YouTube') , 1, 0)) AS footer_social_youtube,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Account Summary') , 1, 0)) AS localnav_acct_summary,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Billing') , 1, 0)) AS localnav_billing,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Profile and Settings') , 1, 0)) AS localnav_settings,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Users') , 1, 0)) AS localnav_users,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('navTabSelect')   AND LOWER(state__view__current_page__elements__ui_name) = LOWER('Voice') , 1, 0)) AS localnav_voice,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('loginStop') , 1, 0)) AS login_attempts_adobe,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('apiCall')   AND LOWER(application__api__api_name) = LOWER('sbNetMemberEdgeV2MembersCreate')   AND LOWER(application__api__response_code) RLIKE '2.*' , 1, 0)) AS new_acct_created_all,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('applicationActivity')   AND LOWER(operation__operation_type) = LOWER('userAddSuccessBanner')   AND LOWER(message__context) = LOWER('Administrator') , 1, 0)) AS new_admin_accounts_created,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('createUsername') , 1, 0)) AS new_ids_created_attempts,
        SUM(IF( FALSE, 1, 0)) AS new_ids_created_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('applicationActivity')   AND LOWER(operation__operation_type) = LOWER('userAddSuccessBanner')   AND LOWER(message__context) = LOWER('Standard') , 1, 0)) AS sub_acct_created,
        SUM(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccess' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE AND message__feature__feature_step_name IN('oneTimePaymentAppSuccess') AND message__feature__feature_step_changed = TRUE)) , 1, 0)) AS one_time_payment_updated,
        SUM(IF( visit__application_details__application_name = 'SMB'  AND message__name = 'featureStop'   AND ((message__feature__feature_step_name IN( 'otpSuccessAutoPay' ) AND message__feature__feature_step_changed = TRUE) OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE AND message__feature__feature_step_name IN('oneTimePaymentAutoPayAppSuccess') AND message__feature__feature_step_changed = TRUE)) , 1, 0)) AS otp_with_autopay_successes,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView') , 1, 0)) AS os_ios_or_android,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView') , 1, 0)) AS os_not_ios_or_android,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('rescheduleAppointmentSuccess') , 1, 0)) AS rescheduled_service_appointments,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND state__search__text IS NOT NULL , 1, 0)) AS search_action,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView') , 1, 0)) AS search_results_clicked,
        SUM(IF( TRUE, 1, 0)) AS site_unique,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')   AND LOWER(message__name) = LOWER('userConfigSet')   AND visit__account__enc_account_number IS NOT NULL , 1, 0)) AS site_unique_auth,
        SUM(IF( LOWER(visit__application_details__application_name)= LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__app_section) = LOWER('support') , 1, 0)) AS support_page_views,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('homeUnauth') , 1, 0)) AS unauth_homepage_page_views,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__page_section__name) = LOWER('header')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('support') , 1, 0)) AS utilitynav_support,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(visit__device__device_type) RLIKE LOWER('and.*') , 1, 0)) AS os_android,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(visit__device__device_type) RLIKE LOWER('ip.*') , 1, 0)) AS os_ios,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(visit__device__device_type) NOT RLIKE LOWER('ip.*')   AND LOWER(visit__device__device_type) NOT RLIKE LOWER('and.*') , 1, 0)) AS os_other,
        SUM(IF( LOWER(visit__application_details__application_name)= LOWER('SMB')  AND LOWER(message__name) = LOWER('pageView')   AND LOWER(state__view__current_page__page_name) = LOWER('recoveryEmailSent') , 1, 0)) AS total_id_recovery_attempts,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('passwordRecoveryComplete') , 1, 0)) AS successful_password_resets,
        SUM(IF( LOWER(visit__application_details__application_name) = LOWER('SMB')  AND LOWER(message__name) = LOWER('selectAction')   AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('resetAccountPassword') , 1, 0)) AS password_reset_attempts,
        SUM(IF( LOWER(visit__application_details__application_name) = 'smb'   AND message__name = 'featureStop'    AND operation__success = 'false'   AND message__feature__feature_step_name = 'oneTimePaymentStopFailure' , 1, 0)) AS otp_with_autopay_failures,
        'asp' AS platform,
        'sb' AS domain,
        
        week_ending_date as week_ending_date_denver
    FROM asp_v_venona_events_portals_smb
         LEFT JOIN prod_lkp.week_ending ON epoch_converter(cast(received__timestamp as bigint),'America/Denver') = partition_date
    WHERE (week_ending_date >= '${env:START_DATE}'
       AND week_ending_date <  '${env:END_DATE}')
    GROUP BY
        week_ending_date,
        
        COALESCE (visit__account__details__mso,'Unknown')
    ;
INSERT OVERWRITE TABLE prod.asp_venona_counts_weekly
PARTITION(unit,platform,domain,company,week_ending_date_denver,source_table)

    SELECT  value,
            metric,
            
            'instances',
            'asp',
            'sb',
            company,
            week_ending_date_denver,
            'asp_v_venona_events_portals_smb'
    FROM (SELECT  company,
                  week_ending_date_denver,
                  
                  MAP(

                      'auto_pay_setup_successes|Auto Pay Enrollment Successes|SpectrumBusiness.net||', auto_pay_setup_successes,
                      'view_online_statement|View Online Statement|SpectrumBusiness.net||', view_online_statement,
                      'username_recovery_attempts|Attempts to Recover ID|SpectrumBusiness.net||', username_recovery_attempts,
                      'reset_password_attempts|Attempts to Reset Password|SpectrumBusiness.net||', reset_password_attempts,
                      'auth_homepage_page_views|Authenticated Homepage Page Views|SpectrumBusiness.net||', auth_homepage_page_views,
                      'cancelled_service_appointments|Cancelled Service Appointments|SpectrumBusiness.net||', cancelled_service_appointments,
                      'combined_payment_successes|Combined One Time Payment and Auto Pay with One Time Payment Successes|SpectrumBusiness.net||', combined_payment_successes,
                      'contact_us_page_views|Contact Us Page Views|SpectrumBusiness.net||', contact_us_page_views,
                      'site_unique_mobile|Device Count Mobile|SpectrumBusiness.net||', site_unique_mobile,
                      'site_unique_pc|Device Count PC|SpectrumBusiness.net||', site_unique_pc,
                      'footer_manageaccount_account_summary|Footer - Account Summary|SpectrumBusiness.net||', footer_manageaccount_account_summary,
                      'footer_manageaccount_billing|Footer - Billing|SpectrumBusiness.net||', footer_manageaccount_billing,
                      'footer_legal_ca_privacy_rights|Footer - CA Privacy Rights|SpectrumBusiness.net||', footer_legal_ca_privacy_rights,
                      'footer_charter_corporate_about_us|Footer - Charter Corporate About Us|SpectrumBusiness.net||', footer_charter_corporate_about_us,
                      'footer_charter_corporate_guarantee|Footer - Charter Corporate Guarantee|SpectrumBusiness.net||', footer_charter_corporate_guarantee,
                      'footer_contactus_contact_us|Footer - Contact Us|SpectrumBusiness.net||', footer_contactus_contact_us,
                      'footer_social_facebook|Footer - Facebook|SpectrumBusiness.net||', footer_social_facebook,
                      'footer_contactus_find_spectrum_store|Footer - Find a Spectrum Store|SpectrumBusiness.net||', footer_contactus_find_spectrum_store,
                      'footer_contactus_give_website_feedback|Footer - Give Website Feedback |SpectrumBusiness.net||', footer_contactus_give_website_feedback,
                      'footer_social_linkedin|Footer - LinkedIn|SpectrumBusiness.net||', footer_social_linkedin,
                      'footer_manageaccount_manage_users|Footer - Manage Users|SpectrumBusiness.net||', footer_manageaccount_manage_users,
                      'footer_legal_policies|Footer - Policies|SpectrumBusiness.net||', footer_legal_policies,
                      'footer_legal_privacy_rights|Footer - Privacy Rights|SpectrumBusiness.net||', footer_legal_privacy_rights,
                      'footer_manageaccount_settings|Footer - Settings|SpectrumBusiness.net||', footer_manageaccount_settings,
                      'footer_social_twitter|Footer - Twitter|SpectrumBusiness.net||', footer_social_twitter,
                      'footer_manageaccount_voice|Footer - Voice|SpectrumBusiness.net||', footer_manageaccount_voice,
                      'footer_contactus_weather_outage|Footer - Weather Outage|SpectrumBusiness.net||', footer_contactus_weather_outage,
                      'footer_social_youtube|Footer - Youtube|SpectrumBusiness.net||', footer_social_youtube,
                      'localnav_acct_summary|LocalNav - Account Summary|SpectrumBusiness.net||', localnav_acct_summary,
                      'localnav_billing|LocalNav - Billing|SpectrumBusiness.net||', localnav_billing,
                      'localnav_settings|LocalNav - Settings|SpectrumBusiness.net||', localnav_settings,
                      'localnav_users|LocalNav - Users|SpectrumBusiness.net||', localnav_users,
                      'localnav_voice|LocalNav - Voice|SpectrumBusiness.net||', localnav_voice,
                      'login_attempts_adobe|Login Attempts|SpectrumBusiness.net||', login_attempts_adobe,
                      'new_acct_created_all|New Accounts Created - All|SpectrumBusiness.net||', new_acct_created_all,
                      'new_admin_accounts_created|New Admin Accounts Created|SpectrumBusiness.net||', new_admin_accounts_created,
                      'new_ids_created_attempts|New IDs Created Attempts|SpectrumBusiness.net||', new_ids_created_attempts,
                      'new_ids_created_successes|New IDs Created Successes|SpectrumBusiness.net||', new_ids_created_successes,
                      'sub_acct_created|New Sub Accounts Created|SpectrumBusiness.net||', sub_acct_created,
                      'one_time_payment_updated|One Time Payment Successes|SpectrumBusiness.net||', one_time_payment_updated,
                      'otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|SpectrumBusiness.net||', otp_with_autopay_successes,
                      'os_ios_or_android|Operating System - iOS or Android|SpectrumBusiness.net||', os_ios_or_android,
                      'os_not_ios_or_android|Operating System - Not iOS or Android|SpectrumBusiness.net||', os_not_ios_or_android,
                      'rescheduled_service_appointments|Rescheduled Service Appointments|SpectrumBusiness.net||', rescheduled_service_appointments,
                      'search_action|Search Action|SpectrumBusiness.net||', search_action,
                      'search_results_clicked|Search Results Clicked|SpectrumBusiness.net||', search_results_clicked,
                      'site_unique|Site Unique Values|SpectrumBusiness.net||', site_unique,
                      'site_unique_auth|Site Unique Values Authenticated|SpectrumBusiness.net||', site_unique_auth,
                      'support_page_views|Support Section Page Views|SpectrumBusiness.net||', support_page_views,
                      'unauth_homepage_page_views|Unauthenticated Homepage Page Views|SpectrumBusiness.net||', unauth_homepage_page_views,
                      'utilitynav_support|Utility Navigation - Support|SpectrumBusiness.net||', utilitynav_support,
                      'os_android|Operating System - Android|SpectrumBusiness.net||', os_android,
                      'os_ios|Operating System - iOS|SpectrumBusiness.net||', os_ios,
                      'os_other|Operating System - Not Android or iOS|SpectrumBusiness.net||', os_other,
                      'total_id_recovery_attempts|Total ID Recovery Attempts|SpectrumBusiness.net||', total_id_recovery_attempts,
                      'successful_password_resets|Successful Password Resets|SpectrumBusiness.net||', successful_password_resets,
                      'password_reset_attempts|Password Reset Attempts|SpectrumBusiness.net||', password_reset_attempts,
                      'otp_with_autopay_failures|One Time Payment with Auto Pay Enrollment Failures|SpectrumBusiness.net||', otp_with_autopay_failures
                  ) as map_column
          FROM ${env:TMP_db}.asp_venona_counts_weekly_counts_instances_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
