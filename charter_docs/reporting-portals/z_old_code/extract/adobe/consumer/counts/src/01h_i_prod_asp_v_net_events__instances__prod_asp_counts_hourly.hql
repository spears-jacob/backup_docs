set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_counts_hourly_counts_instances_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_counts_hourly_counts_instances_columns AS
    SELECT
        'L-CHTR' as company,
        SUM(IF( message__category = 'Page View'  AND state__view__current_page__name = 'home-authenticated' , 1, 0)) AS auth_homepage_page_views,
        SUM(IF( FALSE, 1, 0)) AS set_up_auto_payment_failures,
        SUM(IF( FALSE, 1, 0)) AS set_up_auto_payment_successes,
        SUM(IF( state__content__details__episode_title RLIKE '.*bp:autopay.*', 1, 0)) AS autopay_is_enabled,
        SUM(IF( state__content__details__episode_title RLIKE '.*bp:no-autopay.*', 1, 0)) AS autopay_is_not_enabled,
        SUM(IF( state__content__details__episode_title RLIKE '.*pastDue-no.*', 1, 0)) AS bill_status_past_due_no,
        SUM(IF( state__content__details__episode_title RLIKE '.*pastDue-yes.*', 1, 0)) AS bill_status_past_due_yes,
        SUM(IF( FALSE, 1, 0)) AS cancelled_service_appointments,
        SUM(IF( message__category = 'Page View'  AND LOWER(message__name) = 'support.category.general.contact-us' , 1, 0)) AS contact_us_page_views,
        SUM(IF( visit__settings['post_prop48'] IN ('https://www.spectrum.net/my-account/create/#/account-info?SourceApp=MSA'), 1, 0)) AS create_username_referred_from_msa,
        SUM(IF( FALSE, 1, 0)) AS equipment_page_views,
        SUM(IF( message__category = 'Page View'  AND message__name = 'Reset-Equipment-Internet-Failure' , 1, 0)) AS internet_equipment_reset_flow_failures,
        SUM(IF( message__category = 'Page View'  AND message__name = 'reset-equipment-tv-failure' , 1, 0)) AS tv_equipment_reset_flow_failures,
        SUM(IF( message__category = 'Page View'  AND message__name = 'Reset-Equipment-Voice-Failure' , 1, 0)) AS voice_equipment_reset_flow_failures,
        SUM(IF( message__category = 'Custom Link'  AND message__name = 'equip.internet.troubleshoot.reset-equip.start' , 1, 0)) AS internet_equipment_reset_flow_starts,
        SUM(IF( message__category = 'Custom Link'  AND message__name = 'equip.tv.troubleshoot.reset-equip.start' , 1, 0)) AS tv_equipment_reset_flow_starts,
        SUM(IF( message__category = 'Custom Link'  AND message__name = 'equip.voice.troubleshoot.reset-equip.start' , 1, 0)) AS voice_equipment_reset_flow_starts,
        SUM(IF( message__category = 'Page View'  AND message__name = 'Reset-Equipment-Internet-Success' , 1, 0)) AS internet_equipment_reset_flow_successes,
        SUM(IF( message__category = 'Page View'  AND message__name = 'reset-equipment-tv-success' , 1, 0)) AS tv_equipment_reset_flow_successes,
        SUM(IF( message__category = 'Page View'  AND message__name = 'Reset-Equipment-Voice-Success' , 1, 0)) AS voice_equipment_reset_flow_successes,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-accountsummary') , 1, 0)) AS footer_manageaccount_account_summary,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-getsupport-accountsupport') , 1, 0)) AS footer_getsupport_acct_support,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-billing') , 1, 0)) AS footer_manageaccount_billing,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-legal-californiaprivacyrights') , 1, 0)) AS footer_legal_ca_privacy_rights,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-contact-contactus') , 1, 0)) AS footer_contactus_contact_us,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-disabilityassistance') , 1, 0)) AS footer_disability_assistance,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-social-facebook') , 1, 0)) AS footer_social_facebook,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-contact-findstore') , 1, 0)) AS footer_contactus_find_spectrum_store,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) = LOWER('footer-contact-websiteFeedback') , 1, 0)) AS footer_contactus_give_website_feedback,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-legal-gotoassist') , 1, 0)) AS footer_legal_go_to_assist,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-watchtv-guide') , 1, 0)) AS footer_watchtv_guide,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-social-instagram') , 1, 0)) AS footer_social_instagram,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-getsupport-internet') , 1, 0)) AS footer_getsupport_internet,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-internet') , 1, 0)) AS footer_manageaccount_internet,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-watchtv-livetv') , 1, 0)) AS footer_watchtv_live_tv,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) = LOWER('footer-manageAccount-mobile') , 1, 0)) AS footer_manageaccount_mobile,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-watchtv-mylibrary') , 1, 0)) AS footer_watchtv_my_library,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-watchtv-ondemand') , 1, 0)) AS footer_watchtv_on_demand,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-legal-policies') , 1, 0)) AS footer_legal_policies,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-legal-privacyrights') , 1, 0)) AS footer_legal_privacy_rights,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-settings') , 1, 0)) AS footer_manageaccount_settings,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-getsupport-supporthome') , 1, 0)) AS footer_getsupport_support_home,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-getsupport-tv') , 1, 0)) AS footer_getsupport_tv,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-tv') , 1, 0)) AS footer_manageaccount_tv,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-watchtv-dvr') , 1, 0)) AS footer_watchtv_dvr,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-social-twitter') , 1, 0)) AS footer_social_twitter,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-getsupport-voice') , 1, 0)) AS footer_getsupport_voice,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-voice') , 1, 0)) AS footer_manageaccount_voice,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-contact-weatheroutage') , 1, 0)) AS footer_contactus_weather_outage,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-social-youtube') , 1, 0)) AS footer_social_youtube,
        SUM(IF( visit__settings['post_prop48'] IN ('https://www.spectrum.net/forgot/#/password?SourceApp=MSA'), 1, 0)) AS forgot_password_referred_from_msaID/Auth,
        SUM(IF( visit__settings['post_prop48'] IN ('https://www.spectrum.net/forgot/#/username?SourceApp=MSA'), 1, 0)) AS forgot_username_referred_from_msa,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-closesidenav') , 1, 0)) AS hamburger_closesidenav,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-createusername') , 1, 0)) AS hamburger_create_username,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-email') , 1, 0)) AS hamburger_email,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-getsupport') , 1, 0)) AS hamburger_get_support,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-manageaccount') , 1, 0)) AS hamburger_manage_account,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-signin') , 1, 0)) AS hamburger_signin,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-signout') , 1, 0)) AS hamburger_signout,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-voice') , 1, 0)) AS hamburger_voice,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-watchtv') , 1, 0)) AS hamburger_watch_tv,
        SUM(IF( FALSE, 1, 0)) AS iva_opens,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-accountsummary') , 1, 0)) AS localnav_acct_summary,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-billing') , 1, 0)) AS localnav_billing,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-internet') , 1, 0)) AS localnav_internet,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-settings') , 1, 0)) AS localnav_settings,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-tv') , 1, 0)) AS localnav_tv,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-voice') , 1, 0)) AS localnav_voice,
        SUM(IF( FALSE, 1, 0)) AS login_attempts,
        SUM(IF( FALSE, 1, 0)) AS manual_reset_failures,
        SUM(IF( FALSE, 1, 0)) AS manual_reset_starts,
        SUM(IF( FALSE, 1, 0)) AS manual_reset_success,
        SUM(IF( FALSE, 1, 0)) AS manual_troubleshoot_page_views,
        SUM(IF( FALSE, 1, 0)) AS internet_modem_reset_failures,
        SUM(IF( FALSE, 1, 0)) AS voice_modem_reset_failures,
        SUM(IF( FALSE, 1, 0)) AS internet_modem_reset_successes,
        SUM(IF( FALSE, 1, 0)) AS voice_modem_reset_successes,
        SUM(IF( FALSE, 1, 0)) AS internet_modem_router_reset_starts,
        SUM(IF( FALSE, 1, 0)) AS voice_modem_router_reset_starts,
        SUM(IF( FALSE, 1, 0)) AS one_time_payment_failures,
        SUM(IF( FALSE, 1, 0)) AS one_time_payment_starts,
        SUM(IF( FALSE, 1, 0)) AS one_time_payment_successes,
        SUM(IF( FALSE, 1, 0)) AS otp_with_autopay_failures,
        SUM(IF( FALSE, 1, 0)) AS otp_with_autopay_successes,
        SUM(IF( FALSE, 1, 0)) AS os_all,
        SUM(IF( FALSE, 1, 0)) AS os_android,
        SUM(IF( FALSE, 1, 0)) AS os_ios,
        SUM(IF( FALSE, 1, 0)) AS os_ios_or_android,
        SUM(IF( FALSE, 1, 0)) AS os_not_ios_or_android,
        SUM(IF( state__content__details__episode_title RLIKE '.*pl:pl-yes.*', 1, 0)) AS paperless_billing_is_enabled,
        SUM(IF( state__content__details__episode_title RLIKE '.*pl:pl-no.*', 1, 0)) AS paperless_billing_is_not_enabled,
        SUM(IF( (( message__category = 'Page View'  AND state__view__current_page__name IN('Reset-1.bam','Reset-1.btm')) OR (message__category = 'Page View' AND state__view__current_page__name = 'Reset-1.nbtm')), 1, 0)) AS password_reset_attempts,
        SUM(IF( ((message__category = 'Page View' AND state__view__current_page__name IN ('Reset-final.bam','Reset-final.btm')) Or (message__category = 'Page View' AND state__view__current_page__name = 'Reset-final.nbtm')), 1, 0)) AS password_reset_successes,
        SUM(IF( FALSE, 1, 0)) AS refresh_set_top_box_failures,
        SUM(IF( FALSE, 1, 0)) AS refresh_set_top_box_requests,
        SUM(IF( FALSE, 1, 0)) AS rescheduled_service_appointments,
        SUM(IF( FALSE, 1, 0)) AS internet_router_reset_failures,
        SUM(IF( FALSE, 1, 0)) AS voice_router_reset_failures,
        SUM(IF( FALSE, 1, 0)) AS internet_router_reset_successes,
        SUM(IF( FALSE, 1, 0)) AS voice_router_reset_successes,
        SUM(IF( state__view__current_page__search_text IS NOT NULL, 1, 0)) AS search_action,
        SUM(IF( FALSE, 1, 0)) AS search_results_clicked,
        SUM(IF( TRUE, 1, 0)) AS site_unique,
        SUM(IF( FALSE, 1, 0)) AS site_unique_auth,
        SUM(IF( message__category = 'Page View'  AND LOWER(state__view__current_page__section) = 'support' , 1, 0)) AS support_section_page_views,
        SUM(IF( message__category = 'Page View'  AND state__view__current_page__name = 'home-unauth' , 1, 0)) AS unauth_homepage_page_views,
        SUM(IF( ((message__category = 'Page View' AND state__view__current_page__name IN('Recover-1.btm','Recover-1.bam')) Or (message__category = 'Page View' AND state__view__current_page__name = 'Recover-1.nbtm' AND state__view__current_page__name NOT IN  ('Recover-noID.nbtm'))), 1, 0)) AS username_recovery_attempts,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('utilitynav-email') , 1, 0)) AS utilitynav_email,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('utilitynav-support') , 1, 0)) AS utilitynav_support,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('utilitynav-voice') , 1, 0)) AS utilitynav_voice,
        SUM(IF( ((message__category = 'Custom Link') AND message__name IN('View Current Bill','Statement','View Statement','pay-bill.billing-statement-download') OR (message__category = 'Custom Link' AND message__name IN('pay-bill.view-statements') AND visit__settings['post_prop26'] IS NOT NULL)), 1, 0)) AS view_online_statement,
        SUM(IF( FALSE, 1, 0)) AS webmail_views,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('internet') , 1, 0)) AS yourservicesequip_internet,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('mobile') , 1, 0)) AS yourservicesequip_mobile,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('tv') , 1, 0)) AS yourservicesequip_tv,
        SUM(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('voice') , 1, 0)) AS yourservicesequip_voice,
        SUM(IF( message__category = 'Page View'  AND (LOWER(message__name) = LOWER('Wait-15-Error-Modal-TV') OR LOWER(message__name) = LOWER('Wait-15-Error-Modal-Internet') OR LOWER(message__name) = LOWER('Wait-15-Error-Modal-Voice')) , 1, 0)) AS equipment_resets_too_many_resets_modal_all,
        SUM(IF( message__category = 'Page View'  AND LOWER(message__name) = LOWER('Wait-15-Error-Modal-Internet') , 1, 0)) AS equipment_resets_too_many_resets_modal_internet,
        SUM(IF( message__category = 'Page View'  AND LOWER(message__name) = LOWER('Wait-15-Error-Modal-TV') , 1, 0)) AS equipment_resets_too_many_resets_modal_tv,
        SUM(IF( message__category = 'Page View'  AND LOWER(message__name) = LOWER('Wait-15-Error-Modal-Voice') , 1, 0)) AS equipment_resets_too_many_resets_modal_voice,
        'asp' AS platform,
        'resi' AS domain,
        prod.epoch_datehour(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_hour_denver,
        epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver
    FROM asp_v_net_events
         
    WHERE (partition_date >= '${env:START_DATE}'
       AND partition_date <  '${env:END_DATE}')
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
            'instances',
            'asp',
            'resi',
            company,
            date_denver,
            'asp_v_net_events'
    FROM (SELECT  company,
                  date_denver,
                  date_hour_denver,
                  MAP(

                      'auth_homepage_page_views|Authenticated Homepage Page Views|Spectrum.net_adobe||', auth_homepage_page_views,
                      'set_up_auto_payment_failures|Auto Pay Enrollment Failures|Spectrum.net_adobe||', set_up_auto_payment_failures,
                      'set_up_auto_payment_successes|Auto Pay Enrollment Successes|Spectrum.net_adobe||', set_up_auto_payment_successes,
                      'autopay_is_enabled|Autopay Is Enabled|Spectrum.net_adobe||', autopay_is_enabled,
                      'autopay_is_not_enabled|Autopay Is Not Enabled|Spectrum.net_adobe||', autopay_is_not_enabled,
                      'bill_status_past_due_no|Bill Status Past Due No|Spectrum.net_adobe||', bill_status_past_due_no,
                      'bill_status_past_due_yes|Bill Status Past Due Yes|Spectrum.net_adobe||', bill_status_past_due_yes,
                      'cancelled_service_appointments|Cancelled Service Appointments|Spectrum.net_adobe||', cancelled_service_appointments,
                      'contact_us_page_views|Contact Us Page Views|Spectrum.net_adobe||', contact_us_page_views,
                      'create_username_referred_from_msa|Create Username Referral From MySpectrum App|Spectrum.net_adobe||', create_username_referred_from_msa,
                      'equipment_page_views|Equipment Flow Page Views|Spectrum.net_adobe||', equipment_page_views,
                      'internet_equipment_reset_flow_failures|Equipment Reset Flow Failures Internet|Spectrum.net_adobe||', internet_equipment_reset_flow_failures,
                      'tv_equipment_reset_flow_failures|Equipment Reset Flow Failures TV|Spectrum.net_adobe||', tv_equipment_reset_flow_failures,
                      'voice_equipment_reset_flow_failures|Equipment Reset Flow Failures Voice|Spectrum.net_adobe||', voice_equipment_reset_flow_failures,
                      'internet_equipment_reset_flow_starts|Equipment Reset Flow Starts Internet|Spectrum.net_adobe||', internet_equipment_reset_flow_starts,
                      'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|Spectrum.net_adobe||', tv_equipment_reset_flow_starts,
                      'voice_equipment_reset_flow_starts|Equipment Reset Flow Starts Voice|Spectrum.net_adobe||', voice_equipment_reset_flow_starts,
                      'internet_equipment_reset_flow_successes|Equipment Reset Flow Successes Internet|Spectrum.net_adobe||', internet_equipment_reset_flow_successes,
                      'tv_equipment_reset_flow_successes|Equipment Reset Flow Successes TV|Spectrum.net_adobe||', tv_equipment_reset_flow_successes,
                      'voice_equipment_reset_flow_successes|Equipment Reset Flow Successes Voice|Spectrum.net_adobe||', voice_equipment_reset_flow_successes,
                      'footer_manageaccount_account_summary|Footer - Account Summary|Spectrum.net_adobe||', footer_manageaccount_account_summary,
                      'footer_getsupport_acct_support|Footer - Acct Support|Spectrum.net_adobe||', footer_getsupport_acct_support,
                      'footer_manageaccount_billing|Footer - Billing|Spectrum.net_adobe||', footer_manageaccount_billing,
                      'footer_legal_ca_privacy_rights|Footer - CA Privacy Rights|Spectrum.net_adobe||', footer_legal_ca_privacy_rights,
                      'footer_contactus_contact_us|Footer - Contact Us|Spectrum.net_adobe||', footer_contactus_contact_us,
                      'footer_disability_assistance|Footer - Disability Assistance|Spectrum.net_adobe||', footer_disability_assistance,
                      'footer_social_facebook|Footer - Facebook|Spectrum.net_adobe||', footer_social_facebook,
                      'footer_contactus_find_spectrum_store|Footer - Find a Spectrum Store|Spectrum.net_adobe||', footer_contactus_find_spectrum_store,
                      'footer_contactus_give_website_feedback|Footer - Give Website Feedback |Spectrum.net_adobe||', footer_contactus_give_website_feedback,
                      'footer_legal_go_to_assist|Footer - Go to Assist|Spectrum.net_adobe||', footer_legal_go_to_assist,
                      'footer_watchtv_guide|Footer - Guide|Spectrum.net_adobe||', footer_watchtv_guide,
                      'footer_social_instagram|Footer - Instagram|Spectrum.net_adobe||', footer_social_instagram,
                      'footer_getsupport_internet|Footer - Internet|Spectrum.net_adobe||', footer_getsupport_internet,
                      'footer_manageaccount_internet|Footer - Internet|Spectrum.net_adobe||', footer_manageaccount_internet,
                      'footer_watchtv_live_tv|Footer - Live TV|Spectrum.net_adobe||', footer_watchtv_live_tv,
                      'footer_manageaccount_mobile|Footer - Mobile|Spectrum.net_adobe||', footer_manageaccount_mobile,
                      'footer_watchtv_my_library|Footer - My Library|Spectrum.net_adobe||', footer_watchtv_my_library,
                      'footer_watchtv_on_demand|Footer - On Demand|Spectrum.net_adobe||', footer_watchtv_on_demand,
                      'footer_legal_policies|Footer - Policies|Spectrum.net_adobe||', footer_legal_policies,
                      'footer_legal_privacy_rights|Footer - Privacy Rights|Spectrum.net_adobe||', footer_legal_privacy_rights,
                      'footer_manageaccount_settings|Footer - Settings|Spectrum.net_adobe||', footer_manageaccount_settings,
                      'footer_getsupport_support_home|Footer - Support Home|Spectrum.net_adobe||', footer_getsupport_support_home,
                      'footer_getsupport_tv|Footer - TV|Spectrum.net_adobe||', footer_getsupport_tv,
                      'footer_manageaccount_tv|Footer - TV|Spectrum.net_adobe||', footer_manageaccount_tv,
                      'footer_watchtv_dvr|Footer - TV DVR|Spectrum.net_adobe||', footer_watchtv_dvr,
                      'footer_social_twitter|Footer - Twitter|Spectrum.net_adobe||', footer_social_twitter,
                      'footer_getsupport_voice|Footer - Voice|Spectrum.net_adobe||', footer_getsupport_voice,
                      'footer_manageaccount_voice|Footer - Voice|Spectrum.net_adobe||', footer_manageaccount_voice,
                      'footer_contactus_weather_outage|Footer - Weather Outage|Spectrum.net_adobe||', footer_contactus_weather_outage,
                      'footer_social_youtube|Footer - Youtube|Spectrum.net_adobe||', footer_social_youtube,
                      'forgot_password_referred_from_msa|Forgot Password Referral From MySpectrum App|Spectrum.net_adobe|ID/Auth|', forgot_password_referred_from_msaID/Auth,
                      'forgot_username_referred_from_msa|Forgot Username Referral From MySpectrum App|Spectrum.net_adobe||', forgot_username_referred_from_msa,
                      'hamburger_closesidenav|Hamburger - closeSideNav|Spectrum.net_adobe||', hamburger_closesidenav,
                      'hamburger_create_username|Hamburger - Create Username|Spectrum.net_adobe||', hamburger_create_username,
                      'hamburger_email|Hamburger - Email|Spectrum.net_adobe||', hamburger_email,
                      'hamburger_get_support|Hamburger - Get Support|Spectrum.net_adobe||', hamburger_get_support,
                      'hamburger_manage_account|Hamburger - Manage Account|Spectrum.net_adobe||', hamburger_manage_account,
                      'hamburger_signin|Hamburger - Sign In|Spectrum.net_adobe||', hamburger_signin,
                      'hamburger_signout|Hamburger - Signout|Spectrum.net_adobe||', hamburger_signout,
                      'hamburger_voice|Hamburger - Voice|Spectrum.net_adobe||', hamburger_voice,
                      'hamburger_watch_tv|Hamburger - Watch TV|Spectrum.net_adobe||', hamburger_watch_tv,
                      'iva_opens|IVA Opens|Spectrum.net_adobe||', iva_opens,
                      'localnav_acct_summary|LocalNav - Account Summary|Spectrum.net_adobe||', localnav_acct_summary,
                      'localnav_billing|LocalNav - Billing|Spectrum.net_adobe||', localnav_billing,
                      'localnav_internet|LocalNav - Internet|Spectrum.net_adobe||', localnav_internet,
                      'localnav_settings|LocalNav - Settings|Spectrum.net_adobe||', localnav_settings,
                      'localnav_tv|LocalNav - TV|Spectrum.net_adobe||', localnav_tv,
                      'localnav_voice|LocalNav - Voice|Spectrum.net_adobe||', localnav_voice,
                      'login_attempts|Login Attempts|Spectrum.net_adobe||', login_attempts,
                      'manual_reset_failures|Manual Reset Failures|Spectrum.net_adobe||', manual_reset_failures,
                      'manual_reset_starts|Manual Reset Starts|Spectrum.net_adobe||', manual_reset_starts,
                      'manual_reset_success|Manual Reset Success|Spectrum.net_adobe||', manual_reset_success,
                      'manual_troubleshoot_page_views|Manual Troubleshooting Page Views|Spectrum.net_adobe||', manual_troubleshoot_page_views,
                      'internet_modem_reset_failures|Modem Reset Failures Internet|Spectrum.net_adobe||', internet_modem_reset_failures,
                      'voice_modem_reset_failures|Modem Reset Failures Voice|Spectrum.net_adobe||', voice_modem_reset_failures,
                      'internet_modem_reset_successes|Modem Reset Successes Internet|Spectrum.net_adobe||', internet_modem_reset_successes,
                      'voice_modem_reset_successes|Modem Reset Successes Voice|Spectrum.net_adobe||', voice_modem_reset_successes,
                      'internet_modem_router_reset_starts|Modem Router Reset Starts Internet|Spectrum.net_adobe||', internet_modem_router_reset_starts,
                      'voice_modem_router_reset_starts|Modem Router Reset Starts Voice|Spectrum.net_adobe||', voice_modem_router_reset_starts,
                      'one_time_payment_failures|One Time Payment Failures|Spectrum.net_adobe||', one_time_payment_failures,
                      'one_time_payment_starts|One Time Payment Starts|Spectrum.net_adobe||', one_time_payment_starts,
                      'one_time_payment_successes|One Time Payment Successes|Spectrum.net_adobe||', one_time_payment_successes,
                      'otp_with_autopay_failures|One Time Payment with Auto Pay Enrollment Failures|Spectrum.net_adobe||', otp_with_autopay_failures,
                      'otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|Spectrum.net_adobe||', otp_with_autopay_successes,
                      'os_all|Operating System - All |Spectrum.net_adobe||', os_all,
                      'os_android|Operating System - Android|Spectrum.net_adobe||', os_android,
                      'os_ios|Operating System - iOS|Spectrum.net_adobe||', os_ios,
                      'os_ios_or_android|Operating System - iOS or Android|Spectrum.net_adobe||', os_ios_or_android,
                      'os_not_ios_or_android|Operating System - Not iOS or Android|Spectrum.net_adobe||', os_not_ios_or_android,
                      'paperless_billing_is_enabled|Paperless Billing is Enabled|Spectrum.net_adobe||', paperless_billing_is_enabled,
                      'paperless_billing_is_not_enabled|Paperless Billing Is Not Enabled|Spectrum.net_adobe||', paperless_billing_is_not_enabled,
                      'password_reset_attempts|Password Reset Attempts|Spectrum.net_adobe||', password_reset_attempts,
                      'password_reset_successes|Password Reset Successes|Spectrum.net_adobe||', password_reset_successes,
                      'refresh_set_top_box_failures|Refresh Digital Receivers Failures|Spectrum.net_adobe||', refresh_set_top_box_failures,
                      'refresh_set_top_box_requests|Refresh Digital Receivers Requests|Spectrum.net_adobe||', refresh_set_top_box_requests,
                      'rescheduled_service_appointments|Rescheduled Service Appointments|Spectrum.net_adobe||', rescheduled_service_appointments,
                      'internet_router_reset_failures|Router Reset Failures Internet|Spectrum.net_adobe||', internet_router_reset_failures,
                      'voice_router_reset_failures|Router Reset Failures Voice|Spectrum.net_adobe||', voice_router_reset_failures,
                      'internet_router_reset_successes|Router Reset Successes Internet|Spectrum.net_adobe||', internet_router_reset_successes,
                      'voice_router_reset_successes|Router Reset Successes Voice|Spectrum.net_adobe||', voice_router_reset_successes,
                      'search_action|Search Action|Spectrum.net_adobe||', search_action,
                      'search_results_clicked|Search Results Clicked|Spectrum.net_adobe||', search_results_clicked,
                      'site_unique|Site Unique Values|Spectrum.net_adobe||', site_unique,
                      'site_unique_auth|Site Unique Values Authenticated|Spectrum.net_adobe||', site_unique_auth,
                      'support_section_page_views|Support Section Page Views|Spectrum.net_adobe||', support_section_page_views,
                      'unauth_homepage_page_views|Unauthenticated Homepage Page Views|Spectrum.net_adobe||', unauth_homepage_page_views,
                      'username_recovery_attempts|Username Recovery Attempts|Spectrum.net_adobe||', username_recovery_attempts,
                      'utilitynav_email|Utility Navigation - Email|Spectrum.net_adobe||', utilitynav_email,
                      'utilitynav_support|Utility Navigation - Support|Spectrum.net_adobe||', utilitynav_support,
                      'utilitynav_voice|Utility Navigation - Voice|Spectrum.net_adobe||', utilitynav_voice,
                      'view_online_statement|View Online Statement|Spectrum.net_adobe||', view_online_statement,
                      'webmail_views|Webmail Views|Spectrum.net_adobe||', webmail_views,
                      'yourservicesequip_internet|Your Services Equipment - Internet|Spectrum.net_adobe||', yourservicesequip_internet,
                      'yourservicesequip_mobile|Your Services Equipment - Mobile|Spectrum.net_adobe||', yourservicesequip_mobile,
                      'yourservicesequip_tv|Your Services Equipment - TV|Spectrum.net_adobe||', yourservicesequip_tv,
                      'yourservicesequip_voice|Your Services Equipment - Voice|Spectrum.net_adobe||', yourservicesequip_voice,
                      'equipment_resets_too_many_resets_modal_all|Equipment Resets Too Many Resets Modal All|Spectrum.net_adobe||', equipment_resets_too_many_resets_modal_all,
                      'equipment_resets_too_many_resets_modal_internet|Equipment Resets Too Many Resets Modal Internet|Spectrum.net_adobe||', equipment_resets_too_many_resets_modal_internet,
                      'equipment_resets_too_many_resets_modal_tv|Equipment Resets Too Many Resets Modal TV|Spectrum.net_adobe||', equipment_resets_too_many_resets_modal_tv,
                      'equipment_resets_too_many_resets_modal_voice|Equipment Resets Too Many Resets Modal Voice|Spectrum.net_adobe||', equipment_resets_too_many_resets_modal_voice
                  ) as map_column
          FROM ${env:TMP_db}.asp_counts_hourly_counts_instances_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
