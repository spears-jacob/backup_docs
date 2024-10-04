USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable_fiscal_monthly} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable_fiscal_monthly} VALUES('${env:RUN_DATE}');

USE ${env:ENVIRONMENT};

-- Portals flavor below --

CREATE TABLE IF NOT EXISTS venona_metric_agg_portals
(
  mso STRING,
  application_type STRING,
  device_type STRING,
  connection_type STRING,
  network_status STRING,
  playback_type STRING,
  cust_type STRING,
  stream_subtype String,
  android_type STRING,
  application_group_type STRING,
  stream_id STRING,
  visit_id STRING,
  device_id STRING,
  acct_id STRING,
  unique_stream_id STRING,
  unique_visit_id STRING,
  unique_device_id STRING,
  unique_account_number STRING,
  variant_uuids array<string>,
  episode_number STRING,
  season_number STRING,
  genres array<string>,
  linear_channel_name STRING,
  browser_name STRING,
  browser_user_agent STRING,
  browser_version STRING,
  tms_program_id STRING,
  min_received_timestamp BIGINT,
  max_received_timestamp BIGINT,
  manual_login_success BIGINT,
  manual_login_attempt BIGINT,
  resume_login_success BIGINT,
  resume_login_attempt BIGINT,
  verifier_login_success BIGINT,
  verifier_login_attempt BIGINT,
  manual_duration_ms_list ARRAY<INT>,
  verifier_duration_ms_list ARRAY<INT>,
  token_exchange_attempts BIGINT,
  stream_init_failures BIGINT,
  tune_time_ms_sum BIGINT,
  tune_time_count BIGINT,
  tune_time_ms_list ARRAY<INT>,
  watch_time_ms BIGINT,
  buffering_time_ms BIGINT,
  bitrate_downshifts BIGINT,
  stream_init_starts BIGINT,
  stream_noninit_failures BIGINT,
  bandwidth_consumed_mb DOUBLE,
  bitrate_content_elapsed_ms BIGINT,
  api_search_error_count INT,
  keyword_search INT,
  predictive_search INT,
  search_closed BIGINT,
  search_entered BIGINT,
  search_entered_space BIGINT,
  search_entered_total BIGINT,
  search_response_time_ms_list ARRAY<INT>,
  search_result_selected INT,
  search_result_type_list ARRAY<STRING>,
  search_started BIGINT,
  search_text_length BIGINT,
  zero_results BIGINT,
  zero_results_space BIGINT,
  billing_division STRING,
  billing_id STRING,
  location_city STRING,
  location_state STRING,
  location_zipcode STRING,
  app_version STRING,
  complete_buffering_time_ms_list ARRAY<INT>,
  complete_manual_duration_ms_list ARRAY<INT>,
  complete_tune_time_ms_list ARRAY<INT>,
  complete_verifier_duration_ms_list ARRAY<INT>,
  complete_watch_time_ms ARRAY<INT>,
  nonactive_account_number STRING,
  nonactive_device_id STRING,
  nonactive_stream_id STRING,
  nonactive_visit_id STRING,
  device_model STRING,
  critical_api_attempts INT,
  critical_api_fails INT,
  application_errors INT,
  critical_api_success INT,
  other_api_response_count INT,
  stream_choice STRING,
  content_class STRING,
  billing_division_id STRING,
  billing_combo_key STRING,
  app_vertical STRING,
  application_name STRING,
  logged_in STRING,
  operating_system STRING,
  form_factor STRING,
  billing_status STRING,
  autopay_status STRING,

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
  ccpa_agree_and_continue_clicks INT,
  ccpa_choice_form_direct_pageviews INT,
  ccpa_flow_complete_successes_pageviews INT,
  ccpa_it_request_ticket_submission_server_4xx_error_counts INT,
  ccpa_it_request_ticket_submission_server_5xx_error_counts INT,
  ccpa_it_request_ticket_submission_user_failed_verification_error_counts INT,
  ccpa_kba_identity_question_pageviews INT,
  ccpa_kba_speedbump_warning_continue_clicks INT,
  ccpa_kba_speedbump_warning_pageviews INT,
  ccpa_kba_successfully_authenticated_counts INT,
  ccpa_kba_user_failed_verification_24hrcooldown_error_counts INT,
  ccpa_kba_user_failed_verification_generic_error_counts INT,
  ccpa_kba_user_failed_verification_timeout_error_counts INT,
  ccpa_lexisnexis_server_error_4xx_after_kba_verification_error_counts INT,
  ccpa_lexisnexis_server_error_5xx_after_kba_verification_error_counts INT,
  ccpa_lexisnexis_user_not_found_error_counts INT,
  ccpa_privacy_api_generic_api_failure_error_counts INT,
  ccpa_resident_id_continue_clicks INT,
  ccpa_resident_id_form_pageviews INT,
  portals_access_voice_platform_via_tile INT,
  portals_account_billing_support_views INT,
  portals_account_settings_add_person_page_views INT,
  portals_account_settings_cancel_manage_contact_info INT,
  portals_account_settings_click_add_person INT,
  portals_account_settings_click_cancel_add_or_change_role INT,
  portals_account_settings_click_cancel_add_person INT,
  portals_account_settings_click_change_role INT,
  portals_account_settings_click_change_role_from_manage_people INT,
  portals_account_settings_click_change_to_standard_role INT,
  portals_account_settings_click_delete_user INT,
  portals_account_settings_click_install_wifi_profile INT,
  portals_account_settings_click_manage_contact_info INT,
  portals_account_settings_click_manage_people INT,
  portals_account_settings_click_resend_code INT,
  portals_account_settings_click_resend_invite INT,
  portals_account_settings_click_save_add_admin INT,
  portals_account_settings_click_save_add_person INT,
  portals_account_settings_click_save_add_standard INT,
  portals_account_settings_click_save_contact_info INT,
  portals_account_settings_click_send_verification_phone_call INT,
  portals_account_settings_click_send_verification_via_text INT,
  portals_account_settings_click_verify_email INT,
  portals_account_settings_click_verify_phone_number INT,
  portals_account_settings_error_expired_code INT,
  portals_account_settings_error_incorrect_code INT,
  portals_account_settings_home_page_api_error INT,
  portals_account_settings_home_page_views INT,
  portals_account_settings_manage_contact_info_page_views INT,
  portals_account_settings_manage_people_page_views INT,
  portals_account_settings_verify_identity_click INT,
  portals_account_settings_your_info_change_primary_user_role INT,
  portals_account_settings_your_info_change_role INT,
  portals_account_settings_your_info_role_change_cancel INT,
  portals_account_settings_your_info_role_change_save INT,
  portals_account_settings_your_info_tab_views INT,
  portals_add_internet_banner_click INT,
  portals_add_tv_banner_click INT,
  portals_add_voice_banner_click INT,
  portals_all_equipment_reset_flow_failures INT,
  portals_all_equipment_reset_flow_failures_deprecated_cl68 INT,
  portals_all_equipment_reset_flow_starts INT,
  portals_all_equipment_reset_flow_starts_deprecated_cl68 INT,
  portals_all_equipment_reset_flow_successes INT,
  portals_all_equipment_reset_flow_successes_deprecated_cl68 INT,
  portals_auth_homepage_page_views INT,
  portals_autopay_enroll_radio_toggle INT,
  portals_bill_delivery_pref_exit INT,
  portals_bill_delivery_pref_keep_paperless INT,
  portals_bill_delivery_pref_manage_paperless INT,
  portals_bill_delivery_pref_paper INT,
  portals_bill_delivery_pref_paper_failure INT,
  portals_bill_delivery_pref_paper_success INT,
  portals_bill_delivery_pref_paperless_enroll_from_homepage INT,
  portals_bill_delivery_pref_paperless_failure INT,
  portals_bill_delivery_pref_paperless_submit INT,
  portals_bill_delivery_pref_paperless_submit_from_billinghome INT,
  portals_bill_delivery_pref_paperless_submit_from_homepage INT,
  portals_bill_delivery_pref_paperless_submit_from_paperless INT,
  portals_bill_delivery_pref_paperless_success INT,
  portals_bill_pay_clicks_from_local_nav INT,
  portals_billing_feature_interaction INT,
  portals_biometrics_fingerprint_enroll INT,
  portals_biometrics_fingerprint_toggle_off INT,
  portals_biometrics_fingerprint_toggle_on INT,
  portals_biometrics_login_attempts INT,
  portals_biometrics_login_failures INT,
  portals_biometrics_login_successes INT,
  portals_call_support INT,
  portals_cancel_edit_device_nickname INT,
  portals_cancel_edit_wifi_network INT,
  portals_cancel_edit_wifi_network_changes INT,
  portals_cancel_pause_device INT,
  portals_cancel_remove_device INT,
  portals_cancel_tutorial_account_page_four INT,
  portals_cancel_tutorial_equip_page_three INT,
  portals_cancel_tutorial_otp_page_two INT,
  portals_cancel_tutorial_start_page_one INT,
  portals_cancel_unpause_device INT,
  portals_cancelled_service_appointments INT,
  portals_close_hostname_tooltip INT,
  portals_close_mac_address_tooltip INT,
  portals_close_network_tooltip INT,
  portals_confirm_edit_wifi_network_changes INT,
  portals_confirm_remove_device INT,
  portals_contact_our_moving_specialist INT,
  portals_contact_us_page_views INT,
  portals_cpni_abandon_roadblock_modal INT,
  portals_cpni_abandon_roadblock_page INT,
  portals_cpni_complete_and_fail_modal INT,
  portals_cpni_complete_and_fail_page INT,
  portals_cpni_complete_and_submit_modal INT,
  portals_cpni_complete_and_submit_page INT,
  portals_cpni_verify_account_modal INT,
  portals_cpni_verify_account_page INT,
  portals_cpni_verify_account_success_modal INT,
  portals_cpni_verify_account_success_page INT,
  portals_create_username_flow_failure INT,
  portals_create_username_flow_start INT,
  portals_create_username_flow_success INT,
  portals_csat_campaign_dismiss INT,
  portals_csat_campaign_interaction INT,
  portals_csat_submit_feedback_cancel_written_feedback INT,
  portals_csat_submit_rating_1_very_dissatisfied INT,
  portals_csat_submit_rating_2_dissatisfied INT,
  portals_csat_submit_rating_3_neutral INT,
  portals_csat_submit_rating_4_satisfied INT,
  portals_csat_submit_rating_5_very_satisfied INT,
  portals_csat_submit_rating_all INT,
  portals_csat_submit_written_feedback INT,
  portals_download_wifi_profile_button_click INT,
  portals_download_wifi_profile_button_click_android INT,
  portals_download_wifi_profile_button_click_ios INT,
  portals_download_wifi_profile_success INT,
  portals_download_wifi_profile_success_android INT,
  portals_download_wifi_profile_success_ios INT,
  portals_ed_my_internet_services INT,
  portals_ed_my_tv_services INT,
  portals_ed_my_voice_services INT,
  portals_ed_progress_bar_modal_internet_deprecated_cl68 INT,
  portals_ed_progress_bar_modal_tv_deprecated_cl68 INT,
  portals_ed_progress_bar_modal_voice_deprecated_cl68 INT,
  portals_edit_ssid_click_manage_wifi_network_tile INT,
  portals_edit_ssid_home_screen_prompt INT,
  portals_equipment_cancel_edit_ssid_select_action INT,
  portals_equipment_confirm_edit_ssid_select_action INT,
  portals_equipment_detail_views INT,
  portals_equipment_edit_ssid_select_action INT,
  portals_equipment_feature_interaction INT,
  portals_equipment_list_view INT,
  portals_equipment_page_views INT,
  portals_equipment_reset_flow_submit_all INT,
  portals_equipment_reset_internet_modal_view_deprecated_cl68 INT,
  portals_equipment_reset_tv_modal_view_deprecated_cl68 INT,
  portals_equipment_reset_voice_modal_view_deprecated_cl68 INT,
  portals_equipment_resets_too_many_resets_modal_all_deprecated_cl68 INT,
  portals_equipment_resets_too_many_resets_modal_internet_deprecated_cl68 INT,
  portals_equipment_resets_too_many_resets_modal_tv_deprecated_cl68 INT,
  portals_equipment_resets_too_many_resets_modal_voice_deprecated_cl68 INT,
  portals_equipmentnav_check_email INT,
  portals_footer_accessibility INT,
  portals_footer_charter_corporate_about_us INT,
  portals_footer_charter_corporate_guarantee INT,
  portals_footer_contactus_contact_us INT,
  portals_footer_contactus_find_spectrum_store INT,
  portals_footer_contactus_weather_outage INT,
  portals_footer_getsupport_acct_support INT,
  portals_footer_getsupport_internet INT,
  portals_footer_getsupport_support_home INT,
  portals_footer_getsupport_tv INT,
  portals_footer_getsupport_voice INT,
  portals_footer_globalnav_voice INT,
  portals_footer_legal_ca_privacy_rights INT,
  portals_footer_legal_go_to_assist INT,
  portals_footer_legal_policies INT,
  portals_footer_legal_privacy_rights INT,
  portals_footer_manageaccount_account_summary INT,
  portals_footer_manageaccount_billing INT,
  portals_footer_manageaccount_internet INT,
  portals_footer_manageaccount_manage_users INT,
  portals_footer_manageaccount_mobile INT,
  portals_footer_manageaccount_settings INT,
  portals_footer_manageaccount_tv INT,
  portals_footer_manageaccount_voice INT,
  portals_footer_social_facebook INT,
  portals_footer_social_instagram INT,
  portals_footer_social_linkedin INT,
  portals_footer_social_twitter INT,
  portals_footer_social_youtube INT,
  portals_footer_watchtv_dvr INT,
  portals_footer_watchtv_guide INT,
  portals_footer_watchtv_live_tv INT,
  portals_footer_watchtv_my_library INT,
  portals_footer_watchtv_on_demand INT,
  portals_forgot_password_flow_failure INT,
  portals_forgot_password_flow_start INT,
  portals_forgot_password_success INT,
  portals_forgot_username_flow_failure INT,
  portals_forgot_username_flow_start INT,
  portals_forgot_username_success INT,
  portals_get_moving_info INT,
  portals_guided_tour_click_next INT,
  portals_guided_tour_click_previous INT,
  portals_guided_tour_exit INT,
  portals_guided_tour_initiated_modal_view INT,
  portals_guided_tour_onboard_continue INT,
  portals_guided_tour_onboard_welcome INT,
  portals_guided_tour_one_of_three INT,
  portals_guided_tour_show_me_later INT,
  portals_guided_tour_three_of_three INT,
  portals_guided_tour_two_of_three INT,
  portals_guided_tour_zero_of_three INT,
  portals_hamburger_closesidenav INT,
  portals_hamburger_create_username INT,
  portals_hamburger_email INT,
  portals_hamburger_get_support INT,
  portals_hamburger_manage_account INT,
  portals_hamburger_signin INT,
  portals_hamburger_signout INT,
  portals_hamburger_voice INT,
  portals_hamburger_watch_tv INT,
  portals_install_wifi_profile_start INT,
  portals_install_wifi_profile_success INT,
  portals_internet_equipment_reset_flow_cancel_deprecated_cl68 INT,
  portals_internet_equipment_reset_flow_failure_modal_continue_deprecated_cl68 INT,
  portals_internet_equipment_reset_flow_failures INT,
  portals_internet_equipment_reset_flow_failures_deprecated_cl68 INT,
  portals_internet_equipment_reset_flow_manual_success_deprecated_cl68 INT,
  portals_internet_equipment_reset_flow_modal_wait INT,
  portals_internet_equipment_reset_flow_starts INT,
  portals_internet_equipment_reset_flow_starts_deprecated_cl68 INT,
  portals_internet_equipment_reset_flow_submit INT,
  portals_internet_equipment_reset_flow_submit_deprecated_cl68 INT,
  portals_internet_equipment_reset_flow_success_modal_continue_deprecated_cl68 INT,
  portals_internet_equipment_reset_flow_successes INT,
  portals_internet_equipment_reset_flow_successes_deprecated_cl68 INT,
  portals_internet_modem_reset_failures INT,
  portals_internet_modem_reset_successes INT,
  portals_internet_modem_router_reset_starts INT,
  portals_internet_router_reset_failures INT,
  portals_internet_router_reset_successes INT,
  portals_internet_services_connection_issue_troubleshoot INT,
  portals_internet_support_views INT,
  portals_iva_opens INT,
  portals_localnav_acct_summary INT,
  portals_localnav_billing INT,
  portals_localnav_internet INT,
  portals_localnav_settings INT,
  portals_localnav_tv INT,
  portals_localnav_users INT,
  portals_localnav_voice INT,
  portals_login_attempts INT,
  portals_login_failures INT,
  portals_logout INT,
  portals_manual_reset_failures INT,
  portals_manual_reset_failures_deprecated_cl68 INT,
  portals_manual_reset_starts INT,
  portals_manual_reset_starts_deprecated_cl68 INT,
  portals_manual_reset_success INT,
  portals_manual_reset_success_deprecated_cl68 INT,
  portals_msa_experience_feedback_click_submit_feedback INT,
  portals_msa_experience_feedback_goal_accomplished INT,
  portals_msa_experience_feedback_goal_not_accomplished INT,
  portals_my_services_cloud_backup INT,
  portals_my_services_desktop_security INT,
  portals_my_services_email INT,
  portals_my_services_web_hosting INT,
  portals_navigation_go_to_security INT,
  portals_new_acct_created_all INT,
  portals_new_admin_accounts_attempts INT,
  portals_new_admin_accounts_created INT,
  portals_new_ids_created_attempts INT,
  portals_new_ids_created_successes INT,
  portals_one_time_alert_clicked INT,
  portals_one_time_page_alert_displayed INT,
  portals_one_time_payment_after_posted_due_date INT,
  portals_one_time_payment_amount INT,
  portals_one_time_payment_before_posted_due_date INT,
  portals_one_time_payment_checking INT,
  portals_one_time_payment_checking_account INT,
  portals_one_time_payment_credit INT,
  portals_one_time_payment_credit_debit_card INT,
  portals_one_time_payment_date INT,
  portals_one_time_payment_date_other INT,
  portals_one_time_payment_date_past_due INT,
  portals_one_time_payment_date_today INT,
  portals_one_time_payment_equals_amount_due INT,
  portals_one_time_payment_equals_current_balance INT,
  portals_one_time_payment_equals_past_due_amount INT,
  portals_one_time_payment_failures INT,
  portals_one_time_payment_less_than_amount_due INT,
  portals_one_time_payment_less_than_current_balance INT,
  portals_one_time_payment_less_than_past_due_amount INT,
  portals_one_time_payment_more_than_amount_due INT,
  portals_one_time_payment_more_than_current_balance INT,
  portals_one_time_payment_more_than_past_due_amount INT,
  portals_one_time_payment_on_posted_due_date INT,
  portals_one_time_payment_savings INT,
  portals_one_time_payment_savings_account INT,
  portals_one_time_payment_select_amount INT,
  portals_one_time_payment_select_date INT,
  portals_one_time_payment_select_payment_method INT,
  portals_one_time_payment_starts INT,
  portals_one_time_payment_submission_new_payment_method INT,
  portals_one_time_payment_submission_new_payment_method_not_stored INT,
  portals_one_time_payment_submission_new_payment_method_stored INT,
  portals_one_time_payment_submission_previously_stored_payment_method INT,
  portals_one_time_payment_submits INT,
  portals_one_time_payment_submits_with_ap INT,
  portals_one_time_payment_submits_without_ap INT,
  portals_one_time_payment_successes INT,
  portals_one_time_payment_successes_with_ap_enroll INT,
  portals_one_time_payment_successes_without_ap_enroll INT,
  portals_open_edit_device_nickname INT,
  portals_open_edit_wifi_network INT,
  portals_open_hostname_tooltip INT,
  portals_open_mac_address_tooltip INT,
  portals_open_network_tooltip INT,
  portals_open_pause_device INT,
  portals_open_remove_device INT,
  portals_open_unpause_device INT,
  portals_page_views INT,
  portals_quick_tutorial INT,
  portals_request_callback INT,
  portals_rescheduled_service_appointments INT,
  portals_reset_password_attempts INT,
  portals_router_advanced_settings_click_about_port_forward INT,
  portals_router_advanced_settings_click_add_port_assignment INT,
  portals_router_advanced_settings_click_cancel_dns_change INT,
  portals_router_advanced_settings_click_cancel_port_assignment INT,
  portals_router_advanced_settings_click_create_ip_reservation INT,
  portals_router_advanced_settings_click_delete_or_cancel_delete_port INT,
  portals_router_advanced_settings_click_delete_port INT,
  portals_router_advanced_settings_click_dns INT,
  portals_router_advanced_settings_click_manage_dns INT,
  portals_router_advanced_settings_click_manage_ip_reservations INT,
  portals_router_advanced_settings_click_port_forward INT,
  portals_router_advanced_settings_click_reset_dns_default INT,
  portals_router_advanced_settings_click_save_confirm_dns INT,
  portals_router_advanced_settings_click_spectrum_dns_default INT,
  portals_router_advanced_settings_click_upnp INT,
  portals_router_advanced_settings_click_use_existing_port INT,
  portals_router_advanced_settings_delete_port_assignment_via_swipe INT,
  portals_router_advanced_settings_port_unavailable_modal INT,
  portals_router_advanced_settings_reserve_ip_toggle_off INT,
  portals_router_advanced_settings_reserve_ip_toggle_on INT,
  portals_router_advanced_settings_upnp_toggle_off INT,
  portals_router_advanced_settings_upnp_toggle_on INT,
  portals_save_edit_device_nickname INT,
  portals_save_edit_wifi_network INT,
  portals_scp_click_cancel_pause_device INT,
  portals_scp_click_cancel_unpause_device INT,
  portals_scp_click_confirm_pause_device INT,
  portals_scp_click_confirm_unpause_device INT,
  portals_scp_click_connected_devices_tab INT,
  portals_scp_click_manage_device_tab INT,
  portals_scp_click_not_connected_devices_tab INT,
  portals_scp_click_pause_device INT,
  portals_scp_click_pause_unpause_device_icon INT,
  portals_scp_click_pause_unpause_device_paused_tab INT,
  portals_scp_click_paused_tab INT,
  portals_scp_click_unpause_device INT,
  portals_scp_ddp_click_cancel_pause_unpause_device INT,
  portals_scp_ddp_click_confirm_pause_unpause_device INT,
  portals_scp_ddp_click_hostname_tooltip INT,
  portals_scp_ddp_click_mac_address_tooltip INT,
  portals_scp_ddp_click_network_tooltip INT,
  portals_scp_ddp_click_pause_unpause_device INT,
  portals_scp_ddp_click_remove_device INT,
  portals_scp_ddp_edit_device_nickname INT,
  portals_scp_ddp_edit_device_nickname_cancel INT,
  portals_scp_ddp_save_device_nickname INT,
  portals_scp_device_type_select INT,
  portals_scp_flagged_customers INT,
  portals_scp_remove_device_cancel INT,
  portals_scp_remove_device_confirm INT,
  portals_scp_select_action_refresh INT,
  portals_scp_view_device_details INT,
  portals_security_qa_campaign_interaction INT,
  portals_security_qa_cancel_close INT,
  portals_security_qa_icon_select_action INT,
  portals_security_qa_skip_this INT,
  portals_security_qa_update_security_page_view INT,
  portals_select_actions INT,
  portals_select_individual_connected_device INT,
  portals_select_individual_not_connected_device INT,
  portals_select_individual_paused_device INT,
  portals_set_up_auto_payment_failures INT,
  portals_set_up_auto_payment_starts INT,
  portals_set_up_auto_payment_submits INT,
  portals_set_up_auto_payment_submits_plus_otp INT,
  portals_set_up_auto_payment_successes INT,
  portals_set_up_auto_payment_successes_with_payment INT,
  portals_set_up_auto_payment_successes_without_payment INT,
  portals_settings_ad_marketing_card_select_action INT,
  portals_settings_moving_select_action INT,
  portals_settings_navigate_to_accessibility INT,
  portals_settings_navigate_to_mobile INT,
  portals_settings_navigate_to_vom INT,
  portals_settings_parental_controls_select_action INT,
  portals_settings_sign_in_security_add_person_cancel_modal INT,
  portals_settings_sign_in_security_add_person_save_modal INT,
  portals_settings_sign_in_security_cancel_select_action INT,
  portals_settings_sign_in_security_manage_select_action INT,
  portals_settings_sign_in_security_manage_voice_line_select_action INT,
  portals_settings_sign_in_security_people_add_select_action INT,
  portals_settings_sign_in_security_people_delete_select_action INT,
  portals_settings_sign_in_security_save_select_action INT,
  portals_settings_sign_in_security_subnav_people_select_action INT,
  portals_settings_sign_in_security_voice_access_toggleoff_select_action INT,
  portals_settings_sign_in_security_voice_access_toggleon_select_action INT,
  portals_settings_verify_select_action INT,
  portals_settings_your_info_cancel_select_action INT,
  portals_settings_your_info_change_role_select_action INT,
  portals_settings_your_info_changes_success INT,
  portals_settings_your_info_manage_select_action INT,
  portals_settings_your_info_resend_verify_code INT,
  portals_settings_your_info_save_select_action INT,
  portals_settings_your_info_select_action INT,
  portals_settings_your_info_verify_failure INT,
  portals_settings_your_info_verify_identity_email_modal_view INT,
  portals_settings_your_info_verify_identity_modal_view INT,
  portals_settings_your_info_verify_identity_phone_modal_view INT,
  portals_settings_your_info_verify_success INT,
  portals_site_unique INT,
  portals_site_unique_auth INT,
  portals_site_unique_mobile INT,
  portals_site_unique_pc INT,
  portals_start_pause_device INT,
  portals_start_unpause_device INT,
  portals_sub_acct_created INT,
  portals_sub_user_creation_attempts INT,
  portals_support_article_select INT,
  portals_support_button INT,
  portals_support_header_nav_account INT,
  portals_support_header_nav_home_security INT,
  portals_support_header_nav_internet INT,
  portals_support_header_nav_mobile INT,
  portals_support_header_nav_support_home INT,
  portals_support_header_nav_tv INT,
  portals_support_header_nav_voice INT,
  portals_support_page_views INT,
  portals_tech_tracker_auth_views INT,
  portals_tech_tracker_cancel_attempt_auth INT,
  portals_tech_tracker_cancel_attempt_unauth INT,
  portals_tech_tracker_reschedule_starts_auth INT,
  portals_tech_tracker_reschedule_starts_unauth INT,
  portals_tech_tracker_unauth_views INT,
  portals_tutorial_account_page_five_page_view INT,
  portals_tutorial_account_page_four_page_view INT,
  portals_tutorial_equip_page_three_page_view INT,
  portals_tutorial_otp_page_two_page_view INT,
  portals_tutorial_start_page_one_page_view INT,
  portals_tutorial_views INT,
  portals_tv_equipment_reset_flow_cancel_deprecated_cl68 INT,
  portals_tv_equipment_reset_flow_failure_modal_continue_deprecated_cl68 INT,
  portals_tv_equipment_reset_flow_failures INT,
  portals_tv_equipment_reset_flow_failures_deprecated_cl68 INT,
  portals_tv_equipment_reset_flow_manual_success_deprecated_cl68 INT,
  portals_tv_equipment_reset_flow_modal_wait INT,
  portals_tv_equipment_reset_flow_starts INT,
  portals_tv_equipment_reset_flow_starts_deprecated_cl68 INT,
  portals_tv_equipment_reset_flow_submit INT,
  portals_tv_equipment_reset_flow_submit_deprecated_cl68 INT,
  portals_tv_equipment_reset_flow_success_modal_continue_deprecated_cl68 INT,
  portals_tv_equipment_reset_flow_successes INT,
  portals_tv_equipment_reset_flow_successes_deprecated_cl68 INT,
  portals_tv_modem_reset_failures INT,
  portals_tv_router_reset_failures INT,
  portals_unauth_homepage_page_views INT,
  portals_update_missing_contact_campaign_interaction INT,
  portals_update_missing_contact_cancel_close_window INT,
  portals_update_missing_contact_icon_click INT,
  portals_update_missing_contact_save INT,
  portals_update_missing_contact_save_all INT,
  portals_update_missing_contact_save_and_continue INT,
  portals_update_missing_contact_skip_this INT,
  portals_update_missing_contact_update_all INT,
  portals_upgrade_links_overview INT,
  portals_upgrade_service_internet INT,
  portals_upgrade_service_tv INT,
  portals_upgrade_service_voice INT,
  portals_utilitynav_email INT,
  portals_utilitynav_support INT,
  portals_utilitynav_voice INT,
  portals_vas_tile_desktop_security INT,
  portals_vas_tile_domain_registration INT,
  portals_vas_tile_email INT,
  portals_vas_tile_web_hosting INT,
  portals_video_support_views INT,
  portals_view_connected_devices INT,
  portals_view_edit_device_nickname INT,
  portals_view_edit_wifi_network INT,
  portals_view_hostname_tooltip INT,
  portals_view_mac_address_tooltip INT,
  portals_view_network_tooltip INT,
  portals_view_not_connected_devices INT,
  portals_view_online_statments INT,
  portals_view_pause_device INT,
  portals_view_paused_devices INT,
  portals_view_remove_device INT,
  portals_view_unpause_device INT,
  portals_voice_delete_messages INT,
  portals_voice_equipment_reset_flow_cancel_deprecated_cl68 INT,
  portals_voice_equipment_reset_flow_failure_modal_continue_deprecated_cl68 INT,
  portals_voice_equipment_reset_flow_failures INT,
  portals_voice_equipment_reset_flow_failures_deprecated_cl68 INT,
  portals_voice_equipment_reset_flow_manual_success_deprecated_cl68 INT,
  portals_voice_equipment_reset_flow_modal_wait INT,
  portals_voice_equipment_reset_flow_starts INT,
  portals_voice_equipment_reset_flow_starts_deprecated_cl68 INT,
  portals_voice_equipment_reset_flow_submit INT,
  portals_voice_equipment_reset_flow_submit_deprecated_cl68 INT,
  portals_voice_equipment_reset_flow_success_modal_continue_deprecated_cl68 INT,
  portals_voice_equipment_reset_flow_successes INT,
  portals_voice_equipment_reset_flow_successes_deprecated_cl68 INT,
  portals_voice_go_to_vom INT,
  portals_voice_modem_reset_failures INT,
  portals_voice_modem_reset_successes INT,
  portals_voice_pageviews INT,
  portals_voice_play_messages INT,
  portals_voice_router_reset_failures INT,
  portals_voice_support_views INT,
  portals_voice_update_call_forward_settings INT,
  portals_voice_view_messages INT,
  portals_voice_voicemail_pause_select_action INT,
  portals_voice_voicemail_view_select_action INT,
  portals_wifi_password_views INT,
  portals_wifi_pw_changes INT,
  portals_yourservicesequip_internet INT,
  portals_yourservicesequip_mobile INT,
  portals_yourservicesequip_tv INT,
  portals_yourservicesequip_voice INT,


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
  portals_unique_acct_key STRING,
  activated_experiments map<string,string>,
  technology_type STRING
)
PARTITIONED BY (denver_date STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - With PII (3 Years)')
;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

DROP VIEW IF EXISTS asp_quantum_metric_agg_v;

CREATE VIEW asp_quantum_metric_agg_v AS
select *
from prod.venona_metric_agg_portals;
