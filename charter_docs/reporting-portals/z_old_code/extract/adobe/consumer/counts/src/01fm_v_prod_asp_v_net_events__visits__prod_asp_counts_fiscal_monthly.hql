set hive.vectorized.execution.enabled = true;
set hive.auto.convert.join=false;
SET hive.tez.container.size=20682;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_counts_fiscal_monthly_counts_visits_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_counts_fiscal_monthly_counts_visits_columns AS
    SELECT
        'L-CHTR' as company,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND state__view__current_page__name = 'home-authenticated' , visit__visit_id, Null))) AS auth_homepage_page_views_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS set_up_auto_payment_failures_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS set_up_auto_payment_successes_visits,
        SIZE(COLLECT_SET(IF( state__content__details__episode_title RLIKE '.*bp:autopay.*', visit__visit_id, Null))) AS autopay_is_enabled_visits,
        SIZE(COLLECT_SET(IF( state__content__details__episode_title RLIKE '.*bp:no-autopay.*', visit__visit_id, Null))) AS autopay_is_not_enabled_visits,
        SIZE(COLLECT_SET(IF( state__content__details__episode_title RLIKE '.*pastDue-no.*', visit__visit_id, Null))) AS bill_status_past_due_no_visits,
        SIZE(COLLECT_SET(IF( state__content__details__episode_title RLIKE '.*pastDue-yes.*', visit__visit_id, Null))) AS bill_status_past_due_yes_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS cancelled_service_appointments_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND LOWER(message__name) = 'support.category.general.contact-us' , visit__visit_id, Null))) AS contact_us_page_views_visits,
        SIZE(COLLECT_SET(IF( visit__settings['post_prop48'] IN ('https://www.spectrum.net/my-account/create/#/account-info?SourceApp=MSA'), visit__visit_id, Null))) AS create_username_referred_from_msa_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS equipment_page_views_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND message__name = 'Reset-Equipment-Internet-Failure' , visit__visit_id, Null))) AS internet_equipment_reset_flow_failures_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND message__name = 'reset-equipment-tv-failure' , visit__visit_id, Null))) AS tv_equipment_reset_flow_failures_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND message__name = 'Reset-Equipment-Voice-Failure' , visit__visit_id, Null))) AS voice_equipment_reset_flow_failures_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND message__name = 'equip.internet.troubleshoot.reset-equip.start' , visit__visit_id, Null))) AS internet_equipment_reset_flow_starts_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND message__name = 'equip.tv.troubleshoot.reset-equip.start' , visit__visit_id, Null))) AS tv_equipment_reset_flow_starts_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND message__name = 'equip.voice.troubleshoot.reset-equip.start' , visit__visit_id, Null))) AS voice_equipment_reset_flow_starts_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND message__name = 'Reset-Equipment-Internet-Success' , visit__visit_id, Null))) AS internet_equipment_reset_flow_successes_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND message__name = 'reset-equipment-tv-success' , visit__visit_id, Null))) AS tv_equipment_reset_flow_successes_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND message__name = 'Reset-Equipment-Voice-Success' , visit__visit_id, Null))) AS voice_equipment_reset_flow_successes_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-accountsummary') , visit__visit_id, Null))) AS footer_manageaccount_account_summary_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-getsupport-accountsupport') , visit__visit_id, Null))) AS footer_getsupport_acct_support_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-billing') , visit__visit_id, Null))) AS footer_manageaccount_billing_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-legal-californiaprivacyrights') , visit__visit_id, Null))) AS footer_legal_ca_privacy_rights_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-contact-contactus') , visit__visit_id, Null))) AS footer_contactus_contact_us_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-disabilityassistance') , visit__visit_id, Null))) AS footer_disability_assistance_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-social-facebook') , visit__visit_id, Null))) AS footer_social_facebook_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-contact-findstore') , visit__visit_id, Null))) AS footer_contactus_find_spectrum_store_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) = LOWER('footer-contact-websiteFeedback') , visit__visit_id, Null))) AS footer_contactus_give_website_feedback_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-legal-gotoassist') , visit__visit_id, Null))) AS footer_legal_go_to_assist_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-watchtv-guide') , visit__visit_id, Null))) AS footer_watchtv_guide_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-social-instagram') , visit__visit_id, Null))) AS footer_social_instagram_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-getsupport-internet') , visit__visit_id, Null))) AS footer_getsupport_internet_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-internet') , visit__visit_id, Null))) AS footer_manageaccount_internet_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-watchtv-livetv') , visit__visit_id, Null))) AS footer_watchtv_live_tv_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) = LOWER('footer-manageAccount-mobile') , visit__visit_id, Null))) AS footer_manageaccount_mobile_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-watchtv-mylibrary') , visit__visit_id, Null))) AS footer_watchtv_my_library_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-watchtv-ondemand') , visit__visit_id, Null))) AS footer_watchtv_on_demand_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-legal-policies') , visit__visit_id, Null))) AS footer_legal_policies_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-legal-privacyrights') , visit__visit_id, Null))) AS footer_legal_privacy_rights_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-settings') , visit__visit_id, Null))) AS footer_manageaccount_settings_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-getsupport-supporthome') , visit__visit_id, Null))) AS footer_getsupport_support_home_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-getsupport-tv') , visit__visit_id, Null))) AS footer_getsupport_tv_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-tv') , visit__visit_id, Null))) AS footer_manageaccount_tv_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-watchtv-dvr') , visit__visit_id, Null))) AS footer_watchtv_dvr_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-social-twitter') , visit__visit_id, Null))) AS footer_social_twitter_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-getsupport-voice') , visit__visit_id, Null))) AS footer_getsupport_voice_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-manageaccount-voice') , visit__visit_id, Null))) AS footer_manageaccount_voice_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-contact-weatheroutage') , visit__visit_id, Null))) AS footer_contactus_weather_outage_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('footer-social-youtube') , visit__visit_id, Null))) AS footer_social_youtube_visits,
        SIZE(COLLECT_SET(IF( visit__settings['post_prop48'] IN ('https://www.spectrum.net/forgot/#/password?SourceApp=MSA'), visit__visit_id, Null))) AS forgot_password_referred_from_msa_visits,
        SIZE(COLLECT_SET(IF( visit__settings['post_prop48'] IN ('https://www.spectrum.net/forgot/#/username?SourceApp=MSA'), visit__visit_id, Null))) AS forgot_username_referred_from_msa_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-closesidenav') , visit__visit_id, Null))) AS hamburger_closesidenav_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-createusername') , visit__visit_id, Null))) AS hamburger_create_username_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-email') , visit__visit_id, Null))) AS hamburger_email_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-getsupport') , visit__visit_id, Null))) AS hamburger_get_support_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-manageaccount') , visit__visit_id, Null))) AS hamburger_manage_account_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-signin') , visit__visit_id, Null))) AS hamburger_signin_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-signout') , visit__visit_id, Null))) AS hamburger_signout_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-voice') , visit__visit_id, Null))) AS hamburger_voice_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('hamburger-watchtv') , visit__visit_id, Null))) AS hamburger_watch_tv_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS iva_opens_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-accountsummary') , visit__visit_id, Null))) AS localnav_acct_summary_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-billing') , visit__visit_id, Null))) AS localnav_billing_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-internet') , visit__visit_id, Null))) AS localnav_internet_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-settings') , visit__visit_id, Null))) AS localnav_settings_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-tv') , visit__visit_id, Null))) AS localnav_tv_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('localnav-voice') , visit__visit_id, Null))) AS localnav_voice_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS login_attempts_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS manual_reset_failures_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS manual_reset_starts_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS manual_reset_success_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS manual_troubleshoot_page_views_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS internet_modem_reset_failures_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS voice_modem_reset_failures_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS internet_modem_reset_successes_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS voice_modem_reset_successes_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS internet_modem_router_reset_starts_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS voice_modem_router_reset_starts_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS one_time_payment_failures_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS one_time_payment_starts_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS one_time_payment_successes_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS otp_with_autopay_failures_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS otp_with_autopay_successes_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS os_all_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS os_android_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS os_ios_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS os_ios_or_android_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS os_not_ios_or_android_visits,
        SIZE(COLLECT_SET(IF( state__content__details__episode_title RLIKE '.*pl:pl-yes.*', visit__visit_id, Null))) AS paperless_billing_is_enabled_visits,
        SIZE(COLLECT_SET(IF( state__content__details__episode_title RLIKE '.*pl:pl-no.*', visit__visit_id, Null))) AS paperless_billing_is_not_enabled_visits,
        SIZE(COLLECT_SET(IF( (( message__category = 'Page View'  AND state__view__current_page__name IN('Reset-1.bam','Reset-1.btm')) OR (message__category = 'Page View' AND state__view__current_page__name = 'Reset-1.nbtm')), visit__visit_id, Null))) AS password_reset_attempts_visits,
        SIZE(COLLECT_SET(IF( ((message__category = 'Page View' AND state__view__current_page__name IN ('Reset-final.bam','Reset-final.btm')) Or (message__category = 'Page View' AND state__view__current_page__name = 'Reset-final.nbtm')), visit__visit_id, Null))) AS password_reset_successes_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS refresh_set_top_box_failures_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS refresh_set_top_box_requests_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS rescheduled_service_appointments_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS internet_router_reset_failures_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS voice_router_reset_failures_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS internet_router_reset_successes_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS voice_router_reset_successes_visits,
        SIZE(COLLECT_SET(IF( state__view__current_page__search_text IS NOT NULL, visit__visit_id, Null))) AS search_action_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS search_results_clicked_visits,
        SIZE(COLLECT_SET(IF( TRUE, visit__visit_id, Null))) AS site_unique_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS site_unique_auth_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND LOWER(state__view__current_page__section) = 'support' , visit__visit_id, Null))) AS support_section_page_views_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND state__view__current_page__name = 'home-unauth' , visit__visit_id, Null))) AS unauth_homepage_page_views_visits,
        SIZE(COLLECT_SET(IF( ((message__category = 'Page View' AND state__view__current_page__name IN('Recover-1.btm','Recover-1.bam')) Or (message__category = 'Page View' AND state__view__current_page__name = 'Recover-1.nbtm' AND state__view__current_page__name NOT IN  ('Recover-noID.nbtm'))), visit__visit_id, Null))) AS username_recovery_attempts_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('utilitynav-email') , visit__visit_id, Null))) AS utilitynav_email_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('utilitynav-support') , visit__visit_id, Null))) AS utilitynav_support_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('utilitynav-voice') , visit__visit_id, Null))) AS utilitynav_voice_visits,
        SIZE(COLLECT_SET(IF( ((message__category = 'Custom Link') AND message__name IN('View Current Bill','Statement','View Statement','pay-bill.billing-statement-download') OR (message__category = 'Custom Link' AND message__name IN('pay-bill.view-statements') AND visit__settings['post_prop26'] IS NOT NULL)), visit__visit_id, Null))) AS view_online_statement_visits,
        SIZE(COLLECT_SET(IF( FALSE, visit__visit_id, Null))) AS webmail_views_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('internet') , visit__visit_id, Null))) AS yourservicesequip_internet_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('mobile') , visit__visit_id, Null))) AS yourservicesequip_mobile_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('tv') , visit__visit_id, Null))) AS yourservicesequip_tv_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Custom Link'  AND LOWER(message__name) IN ('voice') , visit__visit_id, Null))) AS yourservicesequip_voice_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND (LOWER(message__name) = LOWER('Wait-15-Error-Modal-TV') OR LOWER(message__name) = LOWER('Wait-15-Error-Modal-Internet') OR LOWER(message__name) = LOWER('Wait-15-Error-Modal-Voice')) , visit__visit_id, Null))) AS equipment_resets_too_many_resets_modal_all_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND LOWER(message__name) = LOWER('Wait-15-Error-Modal-Internet') , visit__visit_id, Null))) AS equipment_resets_too_many_resets_modal_internet_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND LOWER(message__name) = LOWER('Wait-15-Error-Modal-TV') , visit__visit_id, Null))) AS equipment_resets_too_many_resets_modal_tv_visits,
        SIZE(COLLECT_SET(IF( message__category = 'Page View'  AND LOWER(message__name) = LOWER('Wait-15-Error-Modal-Voice') , visit__visit_id, Null))) AS equipment_resets_too_many_resets_modal_voice_visits,
        'asp' AS platform,
        'resi' AS domain,

        fiscal_month as year_fiscal_month_denver
        FROM asp_v_net_events ne
        LEFT JOIN prod_lkp.chtr_fiscal_month fm on ne.partition_date = fm.partition_date
        WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
    GROUP BY
        fiscal_month,

        'L-CHTR'
    ;
INSERT OVERWRITE TABLE prod.asp_counts_fiscal_monthly
PARTITION(unit,platform,domain,company,year_fiscal_month_denver,source_table)

    SELECT  value,
            metric,

            'visits',
            'asp',
            'resi',
            company,
            year_fiscal_month_denver,
            'asp_v_net_events'
    FROM (SELECT  company,
                  year_fiscal_month_denver,

                  MAP(

                      'auth_homepage_page_views|Authenticated Homepage Page Views|Spectrum.net_adobe||', auth_homepage_page_views_visits,
                      'set_up_auto_payment_failures|Auto Pay Enrollment Failures|Spectrum.net_adobe||', set_up_auto_payment_failures_visits,
                      'set_up_auto_payment_successes|Auto Pay Enrollment Successes|Spectrum.net_adobe||', set_up_auto_payment_successes_visits,
                      'autopay_is_enabled|Autopay Is Enabled|Spectrum.net_adobe||', autopay_is_enabled_visits,
                      'autopay_is_not_enabled|Autopay Is Not Enabled|Spectrum.net_adobe||', autopay_is_not_enabled_visits,
                      'bill_status_past_due_no|Bill Status Past Due No|Spectrum.net_adobe||', bill_status_past_due_no_visits,
                      'bill_status_past_due_yes|Bill Status Past Due Yes|Spectrum.net_adobe||', bill_status_past_due_yes_visits,
                      'cancelled_service_appointments|Cancelled Service Appointments|Spectrum.net_adobe||', cancelled_service_appointments_visits,
                      'contact_us_page_views|Contact Us Page Views|Spectrum.net_adobe||', contact_us_page_views_visits,
                      'create_username_referred_from_msa|Create Username Referral From MySpectrum App|Spectrum.net_adobe||', create_username_referred_from_msa_visits,
                      'equipment_page_views|Equipment Flow Page Views|Spectrum.net_adobe||', equipment_page_views_visits,
                      'internet_equipment_reset_flow_failures|Equipment Reset Flow Failures Internet|Spectrum.net_adobe||', internet_equipment_reset_flow_failures_visits,
                      'tv_equipment_reset_flow_failures|Equipment Reset Flow Failures TV|Spectrum.net_adobe||', tv_equipment_reset_flow_failures_visits,
                      'voice_equipment_reset_flow_failures|Equipment Reset Flow Failures Voice|Spectrum.net_adobe||', voice_equipment_reset_flow_failures_visits,
                      'internet_equipment_reset_flow_starts|Equipment Reset Flow Starts Internet|Spectrum.net_adobe||', internet_equipment_reset_flow_starts_visits,
                      'tv_equipment_reset_flow_starts|Equipment Reset Flow Starts TV|Spectrum.net_adobe||', tv_equipment_reset_flow_starts_visits,
                      'voice_equipment_reset_flow_starts|Equipment Reset Flow Starts Voice|Spectrum.net_adobe||', voice_equipment_reset_flow_starts_visits,
                      'internet_equipment_reset_flow_successes|Equipment Reset Flow Successes Internet|Spectrum.net_adobe||', internet_equipment_reset_flow_successes_visits,
                      'tv_equipment_reset_flow_successes|Equipment Reset Flow Successes TV|Spectrum.net_adobe||', tv_equipment_reset_flow_successes_visits,
                      'voice_equipment_reset_flow_successes|Equipment Reset Flow Successes Voice|Spectrum.net_adobe||', voice_equipment_reset_flow_successes_visits,
                      'footer_manageaccount_account_summary|Footer - Account Summary|Spectrum.net_adobe||', footer_manageaccount_account_summary_visits,
                      'footer_getsupport_acct_support|Footer - Acct Support|Spectrum.net_adobe||', footer_getsupport_acct_support_visits,
                      'footer_manageaccount_billing|Footer - Billing|Spectrum.net_adobe||', footer_manageaccount_billing_visits,
                      'footer_legal_ca_privacy_rights|Footer - CA Privacy Rights|Spectrum.net_adobe||', footer_legal_ca_privacy_rights_visits,
                      'footer_contactus_contact_us|Footer - Contact Us|Spectrum.net_adobe||', footer_contactus_contact_us_visits,
                      'footer_disability_assistance|Footer - Disability Assistance|Spectrum.net_adobe||', footer_disability_assistance_visits,
                      'footer_social_facebook|Footer - Facebook|Spectrum.net_adobe||', footer_social_facebook_visits,
                      'footer_contactus_find_spectrum_store|Footer - Find a Spectrum Store|Spectrum.net_adobe||', footer_contactus_find_spectrum_store_visits,
                      'footer_contactus_give_website_feedback|Footer - Give Website Feedback |Spectrum.net_adobe||', footer_contactus_give_website_feedback_visits,
                      'footer_legal_go_to_assist|Footer - Go to Assist|Spectrum.net_adobe||', footer_legal_go_to_assist_visits,
                      'footer_watchtv_guide|Footer - Guide|Spectrum.net_adobe||', footer_watchtv_guide_visits,
                      'footer_social_instagram|Footer - Instagram|Spectrum.net_adobe||', footer_social_instagram_visits,
                      'footer_getsupport_internet|Footer - Internet|Spectrum.net_adobe||', footer_getsupport_internet_visits,
                      'footer_manageaccount_internet|Footer - Internet|Spectrum.net_adobe||', footer_manageaccount_internet_visits,
                      'footer_watchtv_live_tv|Footer - Live TV|Spectrum.net_adobe||', footer_watchtv_live_tv_visits,
                      'footer_manageaccount_mobile|Footer - Mobile|Spectrum.net_adobe||', footer_manageaccount_mobile_visits,
                      'footer_watchtv_my_library|Footer - My Library|Spectrum.net_adobe||', footer_watchtv_my_library_visits,
                      'footer_watchtv_on_demand|Footer - On Demand|Spectrum.net_adobe||', footer_watchtv_on_demand_visits,
                      'footer_legal_policies|Footer - Policies|Spectrum.net_adobe||', footer_legal_policies_visits,
                      'footer_legal_privacy_rights|Footer - Privacy Rights|Spectrum.net_adobe||', footer_legal_privacy_rights_visits,
                      'footer_manageaccount_settings|Footer - Settings|Spectrum.net_adobe||', footer_manageaccount_settings_visits,
                      'footer_getsupport_support_home|Footer - Support Home|Spectrum.net_adobe||', footer_getsupport_support_home_visits,
                      'footer_getsupport_tv|Footer - TV|Spectrum.net_adobe||', footer_getsupport_tv_visits,
                      'footer_manageaccount_tv|Footer - TV|Spectrum.net_adobe||', footer_manageaccount_tv_visits,
                      'footer_watchtv_dvr|Footer - TV DVR|Spectrum.net_adobe||', footer_watchtv_dvr_visits,
                      'footer_social_twitter|Footer - Twitter|Spectrum.net_adobe||', footer_social_twitter_visits,
                      'footer_getsupport_voice|Footer - Voice|Spectrum.net_adobe||', footer_getsupport_voice_visits,
                      'footer_manageaccount_voice|Footer - Voice|Spectrum.net_adobe||', footer_manageaccount_voice_visits,
                      'footer_contactus_weather_outage|Footer - Weather Outage|Spectrum.net_adobe||', footer_contactus_weather_outage_visits,
                      'footer_social_youtube|Footer - Youtube|Spectrum.net_adobe||', footer_social_youtube_visits,
                      'forgot_password_referred_from_msa|Forgot Password Referral From MySpectrum App|Spectrum.net_adobe||', forgot_password_referred_from_msa_visits,
                      'forgot_username_referred_from_msa|Forgot Username Referral From MySpectrum App|Spectrum.net_adobe||', forgot_username_referred_from_msa_visits,
                      'hamburger_closesidenav|Hamburger - closeSideNav|Spectrum.net_adobe||', hamburger_closesidenav_visits,
                      'hamburger_create_username|Hamburger - Create Username|Spectrum.net_adobe||', hamburger_create_username_visits,
                      'hamburger_email|Hamburger - Email|Spectrum.net_adobe||', hamburger_email_visits,
                      'hamburger_get_support|Hamburger - Get Support|Spectrum.net_adobe||', hamburger_get_support_visits,
                      'hamburger_manage_account|Hamburger - Manage Account|Spectrum.net_adobe||', hamburger_manage_account_visits,
                      'hamburger_signin|Hamburger - Sign In|Spectrum.net_adobe||', hamburger_signin_visits,
                      'hamburger_signout|Hamburger - Signout|Spectrum.net_adobe||', hamburger_signout_visits,
                      'hamburger_voice|Hamburger - Voice|Spectrum.net_adobe||', hamburger_voice_visits,
                      'hamburger_watch_tv|Hamburger - Watch TV|Spectrum.net_adobe||', hamburger_watch_tv_visits,
                      'iva_opens|IVA Opens|Spectrum.net_adobe||', iva_opens_visits,
                      'localnav_acct_summary|LocalNav - Account Summary|Spectrum.net_adobe||', localnav_acct_summary_visits,
                      'localnav_billing|LocalNav - Billing|Spectrum.net_adobe||', localnav_billing_visits,
                      'localnav_internet|LocalNav - Internet|Spectrum.net_adobe||', localnav_internet_visits,
                      'localnav_settings|LocalNav - Settings|Spectrum.net_adobe||', localnav_settings_visits,
                      'localnav_tv|LocalNav - TV|Spectrum.net_adobe||', localnav_tv_visits,
                      'localnav_voice|LocalNav - Voice|Spectrum.net_adobe||', localnav_voice_visits,
                      'login_attempts|Login Attempts|Spectrum.net_adobe||', login_attempts_visits,
                      'manual_reset_failures|Manual Reset Failures|Spectrum.net_adobe||', manual_reset_failures_visits,
                      'manual_reset_starts|Manual Reset Starts|Spectrum.net_adobe||', manual_reset_starts_visits,
                      'manual_reset_success|Manual Reset Success|Spectrum.net_adobe||', manual_reset_success_visits,
                      'manual_troubleshoot_page_views|Manual Troubleshooting Page Views|Spectrum.net_adobe||', manual_troubleshoot_page_views_visits,
                      'internet_modem_reset_failures|Modem Reset Failures Internet|Spectrum.net_adobe||', internet_modem_reset_failures_visits,
                      'voice_modem_reset_failures|Modem Reset Failures Voice|Spectrum.net_adobe||', voice_modem_reset_failures_visits,
                      'internet_modem_reset_successes|Modem Reset Successes Internet|Spectrum.net_adobe||', internet_modem_reset_successes_visits,
                      'voice_modem_reset_successes|Modem Reset Successes Voice|Spectrum.net_adobe||', voice_modem_reset_successes_visits,
                      'internet_modem_router_reset_starts|Modem Router Reset Starts Internet|Spectrum.net_adobe||', internet_modem_router_reset_starts_visits,
                      'voice_modem_router_reset_starts|Modem Router Reset Starts Voice|Spectrum.net_adobe||', voice_modem_router_reset_starts_visits,
                      'one_time_payment_failures|One Time Payment Failures|Spectrum.net_adobe||', one_time_payment_failures_visits,
                      'one_time_payment_starts|One Time Payment Starts|Spectrum.net_adobe||', one_time_payment_starts_visits,
                      'one_time_payment_successes|One Time Payment Successes|Spectrum.net_adobe||', one_time_payment_successes_visits,
                      'otp_with_autopay_failures|One Time Payment with Auto Pay Enrollment Failures|Spectrum.net_adobe||', otp_with_autopay_failures_visits,
                      'otp_with_autopay_successes|One Time Payment with Auto Pay Enrollment Successes|Spectrum.net_adobe||', otp_with_autopay_successes_visits,
                      'os_all|Operating System - All |Spectrum.net_adobe||', os_all_visits,
                      'os_android|Operating System - Android|Spectrum.net_adobe||', os_android_visits,
                      'os_ios|Operating System - iOS|Spectrum.net_adobe||', os_ios_visits,
                      'os_ios_or_android|Operating System - iOS or Android|Spectrum.net_adobe||', os_ios_or_android_visits,
                      'os_not_ios_or_android|Operating System - Not iOS or Android|Spectrum.net_adobe||', os_not_ios_or_android_visits,
                      'paperless_billing_is_enabled|Paperless Billing is Enabled|Spectrum.net_adobe||', paperless_billing_is_enabled_visits,
                      'paperless_billing_is_not_enabled|Paperless Billing Is Not Enabled|Spectrum.net_adobe||', paperless_billing_is_not_enabled_visits,
                      'password_reset_attempts|Password Reset Attempts|Spectrum.net_adobe||', password_reset_attempts_visits,
                      'password_reset_successes|Password Reset Successes|Spectrum.net_adobe||', password_reset_successes_visits,
                      'refresh_set_top_box_failures|Refresh Digital Receivers Failures|Spectrum.net_adobe||', refresh_set_top_box_failures_visits,
                      'refresh_set_top_box_requests|Refresh Digital Receivers Requests|Spectrum.net_adobe||', refresh_set_top_box_requests_visits,
                      'rescheduled_service_appointments|Rescheduled Service Appointments|Spectrum.net_adobe||', rescheduled_service_appointments_visits,
                      'internet_router_reset_failures|Router Reset Failures Internet|Spectrum.net_adobe||', internet_router_reset_failures_visits,
                      'voice_router_reset_failures|Router Reset Failures Voice|Spectrum.net_adobe||', voice_router_reset_failures_visits,
                      'internet_router_reset_successes|Router Reset Successes Internet|Spectrum.net_adobe||', internet_router_reset_successes_visits,
                      'voice_router_reset_successes|Router Reset Successes Voice|Spectrum.net_adobe||', voice_router_reset_successes_visits,
                      'search_action|Search Action|Spectrum.net_adobe||', search_action_visits,
                      'search_results_clicked|Search Results Clicked|Spectrum.net_adobe||', search_results_clicked_visits,
                      'site_unique|Site Unique Values|Spectrum.net_adobe||', site_unique_visits,
                      'site_unique_auth|Site Unique Values Authenticated|Spectrum.net_adobe||', site_unique_auth_visits,
                      'support_section_page_views|Support Section Page Views|Spectrum.net_adobe||', support_section_page_views_visits,
                      'unauth_homepage_page_views|Unauthenticated Homepage Page Views|Spectrum.net_adobe||', unauth_homepage_page_views_visits,
                      'username_recovery_attempts|Username Recovery Attempts|Spectrum.net_adobe||', username_recovery_attempts_visits,
                      'utilitynav_email|Utility Navigation - Email|Spectrum.net_adobe||', utilitynav_email_visits,
                      'utilitynav_support|Utility Navigation - Support|Spectrum.net_adobe||', utilitynav_support_visits,
                      'utilitynav_voice|Utility Navigation - Voice|Spectrum.net_adobe||', utilitynav_voice_visits,
                      'view_online_statement|View Online Statement|Spectrum.net_adobe||', view_online_statement_visits,
                      'webmail_views|Webmail Views|Spectrum.net_adobe||', webmail_views_visits,
                      'yourservicesequip_internet|Your Services Equipment - Internet|Spectrum.net_adobe||', yourservicesequip_internet_visits,
                      'yourservicesequip_mobile|Your Services Equipment - Mobile|Spectrum.net_adobe||', yourservicesequip_mobile_visits,
                      'yourservicesequip_tv|Your Services Equipment - TV|Spectrum.net_adobe||', yourservicesequip_tv_visits,
                      'yourservicesequip_voice|Your Services Equipment - Voice|Spectrum.net_adobe||', yourservicesequip_voice_visits,
                      'equipment_resets_too_many_resets_modal_all|Equipment Resets Too Many Resets Modal All|Spectrum.net_adobe||', equipment_resets_too_many_resets_modal_all_visits,
                      'equipment_resets_too_many_resets_modal_internet|Equipment Resets Too Many Resets Modal Internet|Spectrum.net_adobe||', equipment_resets_too_many_resets_modal_internet_visits,
                      'equipment_resets_too_many_resets_modal_tv|Equipment Resets Too Many Resets Modal TV|Spectrum.net_adobe||', equipment_resets_too_many_resets_modal_tv_visits,
                      'equipment_resets_too_many_resets_modal_voice|Equipment Resets Too Many Resets Modal Voice|Spectrum.net_adobe||', equipment_resets_too_many_resets_modal_voice_visits
                  ) as map_column
          FROM ${env:TMP_db}.asp_counts_fiscal_monthly_counts_visits_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
