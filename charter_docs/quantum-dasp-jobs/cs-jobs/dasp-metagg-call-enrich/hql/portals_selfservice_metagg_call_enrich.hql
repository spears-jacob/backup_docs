USE ${env:DASP_db};
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

-- prepare temp tables for all-local processing
DROP TABLE IF EXISTS ${env:TMP_db}.metagg_${env:CLUSTER}_${env:STEP};
CREATE TABLE ${env:TMP_db}.metagg_${env:CLUSTER}_${env:STEP} AS
SELECT * FROM quantum_metric_agg_portals
WHERE (denver_date >= ('${hiveconf:visit_start_date}')
   AND denver_date <  ('${hiveconf:end_date}'))
;

DROP TABLE IF EXISTS ${env:TMP_db}.cpv_${env:CLUSTER}_${env:STEP};
CREATE TABLE ${env:TMP_db}.cpv_${env:CLUSTER}_${env:STEP} AS
SELECT visit_id, count(DISTINCT call_inbound_key) as calls
FROM `${env:CPV}`
WHERE (call_date >= ('${hiveconf:call_start_date}')
   AND call_date <  ('${hiveconf:end_date}'))
GROUP BY visit_id
;

-- run the actual enrichment
insert overwrite table quantum_metric_agg_portals
partition (denver_date)
SELECT
ma.mso AS mso ,
ma.application_type AS application_type ,
ma.device_type AS device_type ,
ma.connection_type AS connection_type ,
ma.network_status AS network_status ,
ma.playback_type AS playback_type ,
ma.cust_type AS cust_type ,
ma.stream_subtype AS stream_subtype ,
ma.android_type AS android_type ,
ma.application_group_type AS application_group_type ,
ma.stream_id AS stream_id ,
ma.visit_id AS visit_id ,
ma.device_id AS device_id ,
ma.acct_id AS acct_id ,
ma.unique_stream_id AS unique_stream_id ,
ma.unique_visit_id AS unique_visit_id ,
ma.unique_device_id AS unique_device_id ,
ma.unique_account_number AS unique_account_number ,
ma.variant_uuids AS variant_uuids ,
ma.episode_number AS episode_number ,
ma.season_number AS season_number ,
ma.genres AS genres ,
ma.linear_channel_name AS linear_channel_name ,
ma.browser_name AS browser_name ,
ma.browser_user_agent AS browser_user_agent ,
ma.browser_version AS browser_version ,
ma.tms_program_id AS tms_program_id ,
ma.min_received_timestamp AS min_received_timestamp,
ma.max_received_timestamp AS max_received_timestamp,
ma.manual_login_success AS manual_login_success,
ma.manual_login_attempt AS manual_login_attempt,
ma.resume_login_success AS resume_login_success,
ma.resume_login_attempt AS resume_login_attempt,
ma.verifier_login_success AS verifier_login_success,
ma.verifier_login_attempt AS verifier_login_attempt,
ma.manual_duration_ms_list AS manual_duration_ms_list ,
ma.verifier_duration_ms_list AS verifier_duration_ms_list ,
ma.token_exchange_attempts AS token_exchange_attempts,
ma.stream_init_failures AS stream_init_failures,
ma.tune_time_ms_sum AS tune_time_ms_sum,
ma.tune_time_count AS tune_time_count,
ma.tune_time_ms_list AS tune_time_ms_list ,
ma.watch_time_ms AS watch_time_ms,
ma.buffering_time_ms AS buffering_time_ms,
ma.bitrate_downshifts AS bitrate_downshifts,
ma.stream_init_starts AS stream_init_starts,
ma.stream_noninit_failures AS stream_noninit_failures,
ma.bandwidth_consumed_mb AS bandwidth_consumed_mb ,
ma.bitrate_content_elapsed_ms AS bitrate_content_elapsed_ms,
ma.api_search_error_count AS api_search_error_count,
ma.keyword_search AS keyword_search,
ma.predictive_search AS predictive_search,
ma.search_closed AS search_closed,
ma.search_entered AS search_entered,
ma.search_entered_space AS search_entered_space,
ma.search_entered_total AS search_entered_total,
ma.search_response_time_ms_list AS search_response_time_ms_list ,
ma.search_result_selected AS search_result_selected,
ma.search_result_type_list AS search_result_type_list,
ma.search_started AS search_started,
ma.search_text_length AS search_text_length,
ma.zero_results AS zero_results,
ma.zero_results_space AS zero_results_space,
ma.billing_division AS billing_division ,
ma.billing_id AS billing_id ,
ma.location_city AS location_city ,
ma.location_state AS location_state ,
ma.location_zipcode AS location_zipcode ,
ma.app_version AS app_version ,
ma.complete_buffering_time_ms_list AS complete_buffering_time_ms_list ,
ma.complete_manual_duration_ms_list AS complete_manual_duration_ms_list ,
ma.complete_tune_time_ms_list AS complete_tune_time_ms_list ,
ma.complete_verifier_duration_ms_list AS complete_verifier_duration_ms_list ,
ma.complete_watch_time_ms AS complete_watch_time_ms ,
ma.nonactive_account_number AS nonactive_account_number ,
ma.nonactive_device_id AS nonactive_device_id ,
ma.nonactive_stream_id AS nonactive_stream_id ,
ma.nonactive_visit_id AS nonactive_visit_id ,
ma.device_model AS device_model ,
ma.critical_api_attempts AS critical_api_attempts,
ma.critical_api_fails AS critical_api_fails,
ma.application_errors AS application_errors,
ma.critical_api_success AS critical_api_success,
ma.other_api_response_count AS other_api_response_count,
ma.stream_choice AS stream_choice ,
ma.content_class AS content_class ,
ma.billing_division_id AS billing_division_id ,
ma.billing_combo_key AS billing_combo_key ,
ma.app_vertical AS app_vertical ,
ma.application_name AS application_name ,
ma.logged_in AS logged_in ,
ma.operating_system AS operating_system ,
ma.form_factor AS form_factor ,
ma.billing_status AS billing_status ,
ma.autopay_status AS autopay_status ,
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
ma.portals_access_voice_platform_via_tile AS   portals_access_voice_platform_via_tile,
ma.portals_account_billing_support_views AS   portals_account_billing_support_views,
ma.portals_account_settings_click_install_wifi_profile AS   portals_account_settings_click_install_wifi_profile,
ma.portals_account_settings_click_manage_people AS   portals_account_settings_click_manage_people,
ma.portals_account_settings_click_save_contact_info AS   portals_account_settings_click_save_contact_info,
ma.portals_account_settings_home_page_views AS   portals_account_settings_home_page_views,
ma.portals_account_settings_manage_contact_info_page_views AS   portals_account_settings_manage_contact_info_page_views,
ma.portals_account_settings_manage_people_page_views AS   portals_account_settings_manage_people_page_views,
ma.portals_account_settings_your_info_tab_views AS   portals_account_settings_your_info_tab_views,
ma.portals_all_equipment_reset_flow_failures AS   portals_all_equipment_reset_flow_failures,
ma.portals_all_equipment_reset_flow_starts AS   portals_all_equipment_reset_flow_starts,
ma.portals_all_equipment_reset_flow_successes AS   portals_all_equipment_reset_flow_successes,
ma.portals_asd_paperless_enroll AS   portals_asd_paperless_enroll,
ma.portals_auth_homepage_page_views AS   portals_auth_homepage_page_views,
ma.portals_bill_delivery_pref_exit AS   portals_bill_delivery_pref_exit,
ma.portals_bill_delivery_pref_keep_paperless AS   portals_bill_delivery_pref_keep_paperless,
ma.portals_bill_delivery_pref_manage_paperless AS   portals_bill_delivery_pref_manage_paperless,
ma.portals_bill_delivery_pref_paper AS   portals_bill_delivery_pref_paper,
ma.portals_bill_delivery_pref_paper_failure AS   portals_bill_delivery_pref_paper_failure,
ma.portals_bill_delivery_pref_paper_success AS   portals_bill_delivery_pref_paper_success,
ma.portals_bill_delivery_pref_paperless_enroll_from_homepage AS   portals_bill_delivery_pref_paperless_enroll_from_homepage,
ma.portals_bill_delivery_pref_paperless_failure AS   portals_bill_delivery_pref_paperless_failure,
ma.portals_bill_delivery_pref_paperless_submit AS   portals_bill_delivery_pref_paperless_submit,
ma.portals_bill_delivery_pref_paperless_submit_from_billinghome AS   portals_bill_delivery_pref_paperless_submit_from_billinghome,
ma.portals_bill_delivery_pref_paperless_submit_from_homepage AS   portals_bill_delivery_pref_paperless_submit_from_homepage,
ma.portals_bill_delivery_pref_paperless_success AS   portals_bill_delivery_pref_paperless_success,
ma.portals_billing_feature_interaction AS   portals_billing_feature_interaction,
ma.portals_billing_feature_page_view AS   portals_billing_feature_page_view,
ma.portals_biometrics_fingerprint_enroll AS   portals_biometrics_fingerprint_enroll,
ma.portals_call_support AS   portals_call_support,
ma.portals_cancel_pause_device AS   portals_cancel_pause_device,
ma.portals_cancel_tutorial_equip_page_three AS   portals_cancel_tutorial_equip_page_three,
ma.portals_cancel_tutorial_otp_page_two AS   portals_cancel_tutorial_otp_page_two,
ma.portals_cancel_tutorial_start_page_one AS   portals_cancel_tutorial_start_page_one,
ma.portals_cancel_unpause_device AS   portals_cancel_unpause_device,
ma.portals_cancelled_service_appointment_start AS   portals_cancelled_service_appointment_start,
ma.portals_cancelled_service_appointments AS   portals_cancelled_service_appointments,
ma.portals_confirm_remove_device AS   portals_confirm_remove_device,
ma.portals_contact_us_page_views AS   portals_contact_us_page_views,
ma.portals_csat_campaign_dismiss AS   portals_csat_campaign_dismiss,
ma.portals_csat_campaign_interaction AS   portals_csat_campaign_interaction,
ma.portals_csat_submit_feedback_cancel_written_feedback AS   portals_csat_submit_feedback_cancel_written_feedback,
ma.portals_csat_submit_rating_1_very_dissatisfied AS   portals_csat_submit_rating_1_very_dissatisfied,
ma.portals_csat_submit_rating_2_dissatisfied AS   portals_csat_submit_rating_2_dissatisfied,
ma.portals_csat_submit_rating_3_neutral AS   portals_csat_submit_rating_3_neutral,
ma.portals_csat_submit_rating_4_satisfied AS   portals_csat_submit_rating_4_satisfied,
ma.portals_csat_submit_rating_5_very_satisfied AS   portals_csat_submit_rating_5_very_satisfied,
ma.portals_csat_submit_rating_all AS   portals_csat_submit_rating_all,
ma.portals_csat_submit_written_feedback AS   portals_csat_submit_written_feedback,
ma.portals_download_wifi_profile_button_click AS   portals_download_wifi_profile_button_click,
ma.portals_download_wifi_profile_button_click_android AS   portals_download_wifi_profile_button_click_android,
ma.portals_download_wifi_profile_button_click_ios AS   portals_download_wifi_profile_button_click_ios,
ma.portals_download_wifi_profile_success AS   portals_download_wifi_profile_success,
ma.portals_download_wifi_profile_success_android AS   portals_download_wifi_profile_success_android,
ma.portals_ed_my_internet_services AS   portals_ed_my_internet_services,
ma.portals_ed_my_voice_services AS   portals_ed_my_voice_services,
ma.portals_equipment_cancel_edit_ssid_select_action AS   portals_equipment_cancel_edit_ssid_select_action,
ma.portals_equipment_confirm_edit_ssid_select_action AS   portals_equipment_confirm_edit_ssid_select_action,
ma.portals_equipment_detail_views AS   portals_equipment_detail_views,
ma.portals_equipment_edit_ssid_select_action AS   portals_equipment_edit_ssid_select_action,
ma.portals_equipment_feature_interaction AS   portals_equipment_feature_interaction,
ma.portals_equipment_list_view AS   portals_equipment_list_view,
ma.portals_equipment_reset_flow_submit_all AS   portals_equipment_reset_flow_submit_all,
ma.portals_footer_getsupport_acct_support AS   portals_footer_getsupport_acct_support,
ma.portals_footer_getsupport_support_home AS   portals_footer_getsupport_support_home,
ma.portals_forgot_password_flow_start AS   portals_forgot_password_flow_start,
ma.portals_forgot_password_success AS   portals_forgot_password_success,
ma.portals_forgot_username_flow_start AS   portals_forgot_username_flow_start,
ma.portals_forgot_username_success AS   portals_forgot_username_success,
ma.portals_guided_tour_click_next AS   portals_guided_tour_click_next,
ma.portals_guided_tour_exit AS   portals_guided_tour_exit,
ma.portals_guided_tour_initiated_modal_view AS   portals_guided_tour_initiated_modal_view,
ma.portals_guided_tour_one_of_three AS   portals_guided_tour_one_of_three,
ma.portals_guided_tour_show_me_later AS   portals_guided_tour_show_me_later,
ma.portals_guided_tour_three_of_three AS   portals_guided_tour_three_of_three,
ma.portals_guided_tour_two_of_three AS   portals_guided_tour_two_of_three,
ma.portals_install_wifi_profile_start AS   portals_install_wifi_profile_start,
ma.portals_install_wifi_profile_success AS   portals_install_wifi_profile_success,
ma.portals_internet_equipment_reset_flow_failures AS   portals_internet_equipment_reset_flow_failures,
ma.portals_internet_equipment_reset_flow_starts AS   portals_internet_equipment_reset_flow_starts,
ma.portals_internet_equipment_reset_flow_submit AS   portals_internet_equipment_reset_flow_submit,
ma.portals_internet_equipment_reset_flow_successes AS   portals_internet_equipment_reset_flow_successes,
ma.portals_internet_modem_reset_failures AS   portals_internet_modem_reset_failures,
ma.portals_internet_modem_router_reset_starts AS   portals_internet_modem_router_reset_starts,
ma.portals_internet_router_reset_failures AS   portals_internet_router_reset_failures,
ma.portals_internet_router_reset_successes AS   portals_internet_router_reset_successes,
ma.portals_internet_support_views AS   portals_internet_support_views,
ma.portals_iva_opens AS   portals_iva_opens,
ma.portals_localnav_billing AS   portals_localnav_billing,
ma.portals_login_attempts AS   portals_login_attempts,
ma.portals_login_failures AS   portals_login_failures,
ma.portals_logout AS   portals_logout,
ma.portals_manage_auto_payment_cancel AS   portals_manage_auto_payment_cancel,
ma.portals_manage_auto_payment_starts AS   portals_manage_auto_payment_starts,
ma.portals_manage_auto_payment_successes AS   portals_manage_auto_payment_successes,
ma.portals_manual_reset_failures AS   portals_manual_reset_failures,
ma.portals_manual_reset_starts AS   portals_manual_reset_starts,
ma.portals_manual_reset_success AS   portals_manual_reset_success,
ma.portals_msa_experience_feedback_click_submit_feedback AS   portals_msa_experience_feedback_click_submit_feedback,
ma.portals_msa_experience_feedback_goal_accomplished AS   portals_msa_experience_feedback_goal_accomplished,
ma.portals_msa_experience_feedback_goal_not_accomplished AS   portals_msa_experience_feedback_goal_not_accomplished,
ma.portals_one_time_alert_clicked AS   portals_one_time_alert_clicked,
ma.portals_one_time_page_alert_displayed AS   portals_one_time_page_alert_displayed,
ma.portals_one_time_payment_amount AS   portals_one_time_payment_amount,
ma.portals_one_time_payment_checking AS   portals_one_time_payment_checking,
ma.portals_one_time_payment_checking_account AS   portals_one_time_payment_checking_account,
ma.portals_one_time_payment_credit AS   portals_one_time_payment_credit,
ma.portals_one_time_payment_credit_debit_card AS   portals_one_time_payment_credit_debit_card,
ma.portals_one_time_payment_date AS   portals_one_time_payment_date,
ma.portals_one_time_payment_date_other AS   portals_one_time_payment_date_other,
ma.portals_one_time_payment_date_past_due AS   portals_one_time_payment_date_past_due,
ma.portals_one_time_payment_date_today AS   portals_one_time_payment_date_today,
ma.portals_one_time_payment_failures AS   portals_one_time_payment_failures,
ma.portals_one_time_payment_savings AS   portals_one_time_payment_savings,
ma.portals_one_time_payment_savings_account AS   portals_one_time_payment_savings_account,
ma.portals_one_time_payment_select_amount AS   portals_one_time_payment_select_amount,
ma.portals_one_time_payment_select_date AS   portals_one_time_payment_select_date,
ma.portals_one_time_payment_select_payment_method AS   portals_one_time_payment_select_payment_method,
ma.portals_one_time_payment_starts AS   portals_one_time_payment_starts,
ma.portals_one_time_payment_submission_new_payment_method AS   portals_one_time_payment_submission_new_payment_method,
ma.portals_one_time_payment_submission_new_payment_method_not_stored AS   portals_one_time_payment_submission_new_payment_method_not_stored,
ma.portals_one_time_payment_submission_new_payment_method_stored AS   portals_one_time_payment_submission_new_payment_method_stored,
ma.portals_one_time_payment_submission_previously_stored_payment_method AS   portals_one_time_payment_submission_previously_stored_payment_method,
ma.portals_one_time_payment_submits AS   portals_one_time_payment_submits,
ma.portals_one_time_payment_submits_with_ap AS   portals_one_time_payment_submits_with_ap,
ma.portals_one_time_payment_submits_without_ap AS   portals_one_time_payment_submits_without_ap,
ma.portals_one_time_payment_successes AS   portals_one_time_payment_successes,
ma.portals_one_time_payment_successes_with_ap_enroll AS   portals_one_time_payment_successes_with_ap_enroll,
ma.portals_one_time_payment_successes_without_ap_enroll AS   portals_one_time_payment_successes_without_ap_enroll,
ma.portals_page_views AS   portals_page_views,
ma.portals_paperless_radio_toggle AS   portals_paperless_radio_toggle,
ma.portals_paperless_toggle AS   portals_paperless_toggle,
ma.portals_rescheduled_service_appointment_start AS   portals_rescheduled_service_appointment_start,
ma.portals_rescheduled_service_appointments AS   portals_rescheduled_service_appointments,
ma.portals_router_advanced_settings_click_about_port_forward AS   portals_router_advanced_settings_click_about_port_forward,
ma.portals_router_advanced_settings_click_add_port_assignment AS   portals_router_advanced_settings_click_add_port_assignment,
ma.portals_router_advanced_settings_click_cancel_dns_change AS   portals_router_advanced_settings_click_cancel_dns_change,
ma.portals_router_advanced_settings_click_cancel_port_assignment AS   portals_router_advanced_settings_click_cancel_port_assignment,
ma.portals_router_advanced_settings_click_create_ip_reservation AS   portals_router_advanced_settings_click_create_ip_reservation,
ma.portals_router_advanced_settings_click_delete_or_cancel_delete_port AS   portals_router_advanced_settings_click_delete_or_cancel_delete_port,
ma.portals_router_advanced_settings_click_delete_port AS   portals_router_advanced_settings_click_delete_port,
ma.portals_router_advanced_settings_click_dns AS   portals_router_advanced_settings_click_dns,
ma.portals_router_advanced_settings_click_manage_dns AS   portals_router_advanced_settings_click_manage_dns,
ma.portals_router_advanced_settings_click_manage_ip_reservations AS   portals_router_advanced_settings_click_manage_ip_reservations,
ma.portals_router_advanced_settings_click_port_forward AS   portals_router_advanced_settings_click_port_forward,
ma.portals_router_advanced_settings_click_reset_dns_default AS   portals_router_advanced_settings_click_reset_dns_default,
ma.portals_router_advanced_settings_click_save_confirm_dns AS   portals_router_advanced_settings_click_save_confirm_dns,
ma.portals_router_advanced_settings_click_spectrum_dns_default AS   portals_router_advanced_settings_click_spectrum_dns_default,
ma.portals_router_advanced_settings_click_upnp AS   portals_router_advanced_settings_click_upnp,
ma.portals_router_advanced_settings_click_use_existing_port AS   portals_router_advanced_settings_click_use_existing_port,
ma.portals_router_advanced_settings_delete_port_assignment_via_swipe AS   portals_router_advanced_settings_delete_port_assignment_via_swipe,
ma.portals_router_advanced_settings_port_unavailable_modal AS   portals_router_advanced_settings_port_unavailable_modal,
ma.portals_router_advanced_settings_reserve_ip_toggle_off AS   portals_router_advanced_settings_reserve_ip_toggle_off,
ma.portals_router_advanced_settings_reserve_ip_toggle_on AS   portals_router_advanced_settings_reserve_ip_toggle_on,
ma.portals_router_advanced_settings_upnp_toggle_off AS   portals_router_advanced_settings_upnp_toggle_off,
ma.portals_router_advanced_settings_upnp_toggle_on AS   portals_router_advanced_settings_upnp_toggle_on,
ma.portals_save_edit_device_nickname AS   portals_save_edit_device_nickname,
ma.portals_scp_click_cancel_pause_device AS   portals_scp_click_cancel_pause_device,
ma.portals_scp_click_cancel_unpause_device AS   portals_scp_click_cancel_unpause_device,
ma.portals_scp_click_not_connected_devices_tab AS   portals_scp_click_not_connected_devices_tab,
ma.portals_scp_click_pause_device AS   portals_scp_click_pause_device,
ma.portals_scp_click_paused_tab AS   portals_scp_click_paused_tab,
ma.portals_scp_click_unpause_device AS   portals_scp_click_unpause_device,
ma.portals_scp_final_confirm_create_pod_name_failure AS   portals_scp_final_confirm_create_pod_name_failure,
ma.portals_scp_final_confirm_create_pod_name_success AS   portals_scp_final_confirm_create_pod_name_success,
ma.portals_scp_final_confirm_device_nickname_change_failure AS   portals_scp_final_confirm_device_nickname_change_failure,
ma.portals_scp_final_confirm_device_nickname_change_success AS   portals_scp_final_confirm_device_nickname_change_success,
ma.portals_scp_final_confirm_device_type_and_nickname_change_failure AS   portals_scp_final_confirm_device_type_and_nickname_change_failure,
ma.portals_scp_final_confirm_device_type_and_nickname_change_success AS   portals_scp_final_confirm_device_type_and_nickname_change_success,
ma.portals_scp_final_confirm_device_type_icon_update_failure AS   portals_scp_final_confirm_device_type_icon_update_failure,
ma.portals_scp_final_confirm_device_type_icon_update_success AS   portals_scp_final_confirm_device_type_icon_update_success,
ma.portals_scp_final_confirm_edit_pod_name_failure AS   portals_scp_final_confirm_edit_pod_name_failure,
ma.portals_scp_final_confirm_edit_pod_name_success AS   portals_scp_final_confirm_edit_pod_name_success,
ma.portals_scp_final_confirm_pause_failure AS   portals_scp_final_confirm_pause_failure,
ma.portals_scp_final_confirm_pause_success AS   portals_scp_final_confirm_pause_success,
ma.portals_scp_final_confirm_refresh_pod_failure AS   portals_scp_final_confirm_refresh_pod_failure,
ma.portals_scp_final_confirm_refresh_pod_success AS   portals_scp_final_confirm_refresh_pod_success,
ma.portals_scp_final_confirm_remove_device_failure AS   portals_scp_final_confirm_remove_device_failure,
ma.portals_scp_final_confirm_remove_device_success AS   portals_scp_final_confirm_remove_device_success,
ma.portals_scp_final_confirm_unpause_failure AS   portals_scp_final_confirm_unpause_failure,
ma.portals_scp_final_confirm_unpause_success AS   portals_scp_final_confirm_unpause_success,
ma.portals_scp_list_of_pods_pv AS   portals_scp_list_of_pods_pv,
ma.portals_scp_pod_details_pv AS   portals_scp_pod_details_pv,
ma.portals_scp_remove_device_cancel AS   portals_scp_remove_device_cancel,
ma.portals_scp_select_pod_from_list AS   portals_scp_select_pod_from_list,
ma.portals_security_qa_campaign_interaction AS   portals_security_qa_campaign_interaction,
ma.portals_security_qa_update_security_page_view AS   portals_security_qa_update_security_page_view,
ma.portals_select_actions AS   portals_select_actions,
ma.portals_set_up_auto_payment_failures AS   portals_set_up_auto_payment_failures,
ma.portals_set_up_auto_payment_starts AS   portals_set_up_auto_payment_starts,
ma.portals_set_up_auto_payment_submits AS   portals_set_up_auto_payment_submits,
ma.portals_set_up_auto_payment_submits_plus_otp AS   portals_set_up_auto_payment_submits_plus_otp,
ma.portals_set_up_auto_payment_successes AS   portals_set_up_auto_payment_successes,
ma.portals_set_up_auto_payment_successes_with_payment AS   portals_set_up_auto_payment_successes_with_payment,
ma.portals_set_up_auto_payment_successes_without_payment AS   portals_set_up_auto_payment_successes_without_payment,
ma.portals_settings_sign_in_security_manage_select_action AS   portals_settings_sign_in_security_manage_select_action,
ma.portals_settings_sign_in_security_subnav_people_select_action AS   portals_settings_sign_in_security_subnav_people_select_action,
ma.portals_settings_verify_select_action AS   portals_settings_verify_select_action,
ma.portals_settings_your_info_changes_success AS   portals_settings_your_info_changes_success,
ma.portals_settings_your_info_manage_select_action AS   portals_settings_your_info_manage_select_action,
ma.portals_settings_your_info_save_select_action AS   portals_settings_your_info_save_select_action,
ma.portals_settings_your_info_verify_identity_modal_view AS   portals_settings_your_info_verify_identity_modal_view,
ma.portals_settings_your_info_verify_success AS   portals_settings_your_info_verify_success,
ma.portals_site_unique AS   portals_site_unique,
ma.portals_site_unique_auth AS   portals_site_unique_auth,
ma.portals_site_unique_mobile AS   portals_site_unique_mobile,
ma.portals_site_unique_pc AS   portals_site_unique_pc,
ma.portals_start_pause_device AS   portals_start_pause_device,
ma.portals_start_unpause_device AS   portals_start_unpause_device,
ma.portals_support_article_select AS   portals_support_article_select,
ma.portals_support_button AS   portals_support_button,
ma.portals_support_page_views AS   portals_support_page_views,
ma.portals_tech_tracker_auth_views AS   portals_tech_tracker_auth_views,
ma.portals_tech_tracker_unauth_views AS   portals_tech_tracker_unauth_views,
ma.portals_tutorial_account_page_five_page_view AS   portals_tutorial_account_page_five_page_view,
ma.portals_tutorial_account_page_four_page_view AS   portals_tutorial_account_page_four_page_view,
ma.portals_tutorial_equip_page_three_page_view AS   portals_tutorial_equip_page_three_page_view,
ma.portals_tutorial_otp_page_two_page_view AS   portals_tutorial_otp_page_two_page_view,
ma.portals_tutorial_start_page_one_page_view AS   portals_tutorial_start_page_one_page_view,
ma.portals_tutorial_views AS   portals_tutorial_views,
ma.portals_tv_equipment_reset_flow_failures AS   portals_tv_equipment_reset_flow_failures,
ma.portals_tv_equipment_reset_flow_starts AS   portals_tv_equipment_reset_flow_starts,
ma.portals_tv_equipment_reset_flow_submit AS   portals_tv_equipment_reset_flow_submit,
ma.portals_tv_equipment_reset_flow_successes AS   portals_tv_equipment_reset_flow_successes,
ma.portals_tv_modem_reset_failures AS   portals_tv_modem_reset_failures,
ma.portals_tv_router_reset_failures AS   portals_tv_router_reset_failures,
ma.portals_unauth_homepage_page_views AS   portals_unauth_homepage_page_views,
ma.portals_update_missing_contact_campaign_interaction AS   portals_update_missing_contact_campaign_interaction,
ma.portals_update_missing_contact_cancel_close_window AS   portals_update_missing_contact_cancel_close_window,
ma.portals_update_missing_contact_save AS   portals_update_missing_contact_save,
ma.portals_update_missing_contact_save_all AS   portals_update_missing_contact_save_all,
ma.portals_update_missing_contact_save_and_continue AS   portals_update_missing_contact_save_and_continue,
ma.portals_update_missing_contact_skip_this AS   portals_update_missing_contact_skip_this,
ma.portals_update_missing_contact_update_all AS   portals_update_missing_contact_update_all,
ma.portals_upgrade_links_overview AS   portals_upgrade_links_overview,
ma.portals_upgrade_service_internet AS   portals_upgrade_service_internet,
ma.portals_video_support_views AS   portals_video_support_views,
ma.portals_view_connected_devices AS   portals_view_connected_devices,
ma.portals_view_online_statments AS   portals_view_online_statments,
ma.portals_voice_delete_messages AS   portals_voice_delete_messages,
ma.portals_voice_equipment_reset_flow_failures AS   portals_voice_equipment_reset_flow_failures,
ma.portals_voice_equipment_reset_flow_starts AS   portals_voice_equipment_reset_flow_starts,
ma.portals_voice_equipment_reset_flow_submit AS   portals_voice_equipment_reset_flow_submit,
ma.portals_voice_equipment_reset_flow_successes AS   portals_voice_equipment_reset_flow_successes,
ma.portals_voice_modem_reset_failures AS   portals_voice_modem_reset_failures,
ma.portals_voice_pageviews AS   portals_voice_pageviews,
ma.portals_voice_play_messages AS   portals_voice_play_messages,
ma.portals_voice_router_reset_failures AS   portals_voice_router_reset_failures,
ma.portals_voice_support_views AS   portals_voice_support_views,
ma.portals_voice_update_call_forward_settings AS   portals_voice_update_call_forward_settings,
ma.portals_voice_view_messages AS   portals_voice_view_messages,
ma.portals_voice_voicemail_view_select_action AS   portals_voice_voicemail_view_select_action,
ma.portals_wifi_password_views AS   portals_wifi_password_views,
ma.portals_wifi_pw_changes AS   portals_wifi_pw_changes,
ma.portals_unique_acct_key AS portals_unique_acct_key,
ma.activated_experiments AS activated_experiments,
ma.technology_type AS technology_type,
COALESCE(cwpv.calls,0) as calls_within_24_hrs,
ma.denver_date AS denver_date
FROM ${env:TMP_db}.metagg_${env:CLUSTER}_${env:STEP} ma
LEFT JOIN ${env:TMP_db}.cpv_${env:CLUSTER}_${env:STEP} cwpv
 ON cwpv.visit_id=ma.visit_id
WHERE 1=1
AND (denver_date >= ('${hiveconf:call_start_date}') AND denver_date < ('${hiveconf:end_date}'))
;

SELECT denver_date, count(denver_date)
FROM quantum_metric_agg_portals
WHERE (denver_date >='${hiveconf:call_start_date}'
  AND  denver_date <  '${hiveconf:end_date}')
AND calls_within_24_hrs > 0
GROUP BY denver_date
ORDER BY denver_date desc
LIMIT 10;

DROP TABLE IF EXISTS ${env:TMP_db}.metagg_${env:CLUSTER}_${env:STEP};
DROP TABLE IF EXISTS ${env:TMP_db}.cpv_${env:CLUSTER}_${env:STEP};
