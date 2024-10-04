-------------------------------------------------------------------------------

--Populates the net_products_agg_monthly table with combined aggregated data

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Combined net_products_agg_monthly Insert --

INSERT OVERWRITE TABLE net_products_agg_monthly PARTITION (year_month)
--PARTITION (year_month = '${env:YEAR_MONTH}')

-- "pam" events fields are sourced from events tables (L-CHTR > net_events, L-BHN > bhn_residential_events, L-TWC > twc_residential_global_events)
-- "fid" federated id fields are sourced from federated_id_auth_attempts_total (L-CHTR). No current L-BHN/L-TWC fields in use
-- "wmc" webmail page views fields are sourced from multiple tables (L-CHTR > net_events, L-BHN/L-TWC > net_webmail_twc_bhn_metrics_monthly)
-- "bpa" bill pay fields are sourced from multiple tables (L-CHTR > net_events, L-BHN > bhn_bill_pay_events, L-TWC > twc_residential_global_events)
-- "nid" new id fields are sourced from multiple tables (L-CHTR > net_events, L-BHN > sbnet_exec_monthly_bhn_accounts_manual, L-TWC > twc_residential_global_events)

SELECT
fid.hhs_logged_in AS hhs_logged_in,
pam.total_hhs AS total_hhs,
pam.percent_hhs_logged_in AS percent_hhs_logged_in,
pam.unique_hhs_mom_change AS unique_hhs_mom_change,
pam.unique_hhs_3month_change AS unique_hhs_3month_change,
--federated id start,
fid.total_login_attempts AS total_login_attempts,
fid.total_login_successes AS total_login_successes,
fid.percent_login_success AS percent_login_success,
--federated id end,
pam.total_page_views_count AS total_page_views_count,
--webmail page views start,
wmc.webmail_page_views_count AS webmail_page_views_count,
--webmail page views end,
pam.my_account_page_views_count AS my_account_page_views_count,
pam.support_page_views_count AS support_page_views_count,
bpa.view_online_statement_count AS view_online_statement_count,
pam.ask_charter_requests_count AS ask_charter_requests_count,
pam.refresh_requests_count AS refresh_requests_count,
pam.video_page_views_count AS video_page_views_count,
pam.video_plays_count AS video_plays_count,
pam.new_ids_charter_count AS new_ids_charter_count,
pam.new_ids_charter_count_bam AS new_ids_charter_count_bam,
pam.new_ids_charter_count_btm AS new_ids_charter_count_btm,
pam.new_ids_charter_count_nbtm AS new_ids_charter_count_nbtm,
pam.attempts_create_id_count_bam AS attempts_create_id_count_bam,
pam.attempts_create_id_count_btm AS attempts_create_id_count_btm,
pam.attempts_create_id_count_nbtm AS attempts_create_id_count_nbtm,
pam.new_ids_not_charter_count AS new_ids_not_charter_count,
pam.total_attempts_id_off_count AS total_attempts_id_off_count,
pam.attempts_recover_username_btm_count AS attempts_recover_username_btm_count,
pam.succesfull_username_recovery_btm_count AS succesfull_username_recovery_btm_count,
pam.attempts_recover_username_bam_count AS attempts_recover_username_bam_count,
pam.succesfull_username_recovery_bam_count AS succesfull_username_recovery_bam_count,
pam.attempts_recover_username_nbtm_count AS attempts_recover_username_nbtm_count,
pam.succesfull_username_recovery_nbtm_count AS succesfull_username_recovery_nbtm_count,
pam.attempts_reset_password_btm_count AS attempts_reset_password_btm_count,
pam.successful_reset_password_btm_count AS successful_reset_password_btm_count,
pam.attempts_reset_password_bam_count AS attempts_reset_password_bam_count,
pam.successful_reset_password_bam_count AS successful_reset_password_bam_count,
pam.attempts_reset_password_nbtm_count AS attempts_reset_password_nbtm_count,
pam.successful_reset_password_nbtm_count AS successful_reset_password_nbtm_count,
pam.attempts_recover_username_password_btm_count AS attempts_recover_username_password_btm_count,
pam.successfully_recover_username_password_btm_count AS successfully_recover_username_password_btm_count,
pam.attempts_recover_username_password_bam_count AS attempts_recover_username_password_bam_count,
pam.successfully_recover_username_password_bam_count AS successfully_recover_username_password_bam_count,
pam.attempts_recover_username_password_nbtm_count AS attempts_recover_username_password_nbtm_count,
pam.successfully_recover_username_password_nbtm_count AS successfully_recover_username_password_nbtm_count,
--bill pay start,
bpa.one_time_payment_count AS one_time_payment_count,
bpa.one_time_payments_confirm_count AS one_time_payments_confirm_count,
bpa.setup_autopay_count AS setup_autopay_count,
bpa.successful_autopay_confirm_count AS successful_autopay_confirm_count,
--bill pay end,
pam.saved_bill_notifications_count AS saved_bill_notifications_count,
pam.saved_appoint_reminders_count AS saved_appoint_reminders_count,
pam.rescheduled_service_appoint_count AS rescheduled_service_appoint_count,
pam.cancelled_service_appoint_count AS cancelled_service_appoint_count,
pam.saved_service_alerts_count AS saved_service_alerts_count,
pam.saved_contact_information_details_count AS saved_contact_information_details_count,
pam.attempts_create_id_count AS attempts_create_id_count,
pam.attempts_reset_password_count AS attempts_reset_password_count,
pam.successful_reset_password_count AS successful_reset_password_count,
nid.new_ids_charter_count_all AS new_ids_charter_count_all,
pam.new_ids_charter_count_on_net AS new_ids_charter_count_on_net,
pam.new_ids_charter_count_off_net AS new_ids_charter_count_off_net,
pam.attempts_create_id_count_all AS attempts_create_id_count_all,
pam.attempts_create_id_count_on_net AS attempts_create_id_count_on_net,
pam.attempts_create_id_count_off_net AS attempts_create_id_count_off_net,
pam.attempts_recover_username_count_all AS attempts_recover_username_count_all,
pam.attempts_recover_username_count_on_net AS attempts_recover_username_count_on_net,
pam.attempts_recover_username_count_off_net AS attempts_recover_username_count_off_net,
pam.successful_username_recovery_count_all AS successful_username_recovery_count_all,
pam.successful_username_recovery_count_on_net AS successful_username_recovery_count_on_net,
pam.successful_username_recovery_count_off_net AS successful_username_recovery_count_off_net,
pam.attempts_reset_password_count_all AS attempts_reset_password_count_all,
pam.attempts_reset_password_count_on_net AS attempts_reset_password_count_on_net,
pam.attempts_reset_password_count_off_net AS attempts_reset_password_count_off_net,
pam.successful_reset_password_count_all AS successful_reset_password_count_all,
pam.successful_reset_password_count_on_net AS successful_reset_password_count_on_net,
pam.successful_reset_password_count_off_net AS successful_reset_password_count_off_net,
pam.attempts_recover_username_password_count_all AS attempts_recover_username_password_count_all,
pam.attempts_recover_username_password_count_on_net AS attempts_recover_username_password_count_on_net,
pam.attempts_recover_username_password_count_off_net AS attempts_recover_username_password_count_off_net,
pam.successfully_recover_username_password_count_all AS successfully_recover_username_password_count_all,
pam.successfully_recover_username_password_count_on_net AS successfully_recover_username_password_count_on_net,
pam.successfully_recover_username_password_count_off_net AS successfully_recover_username_password_count_off_net,
pam.company,
pam.modem_router_resets AS modem_router_resets,
pam.percent_auto_pay_success AS percent_auto_pay_success,
pam.percent_id_recovery_success_off_chtr_network AS percent_id_recovery_success_off_chtr_network,
pam.percent_id_recovery_success_on_chtr_network AS percent_id_recovery_success_on_chtr_network,
pam.percent_new_ids_charter_count_on_net AS percent_new_ids_charter_count_on_net,
pam.percent_one_time_payment_success AS percent_one_time_payment_success,
pam.percent_password_reset_success_off_chtr_network AS percent_password_reset_success_off_chtr_network,
pam.percent_success_new_ids_charter_off_net AS percent_success_new_ids_charter_off_net,
pam.percent_success_recover_reset_username_password_off_net AS percent_success_recover_reset_username_password_off_net,
pam.percent_success_recover_reset_username_password_on_net AS percent_success_recover_reset_username_password_on_net,
pam.percent_success_reset_password_on_net AS percent_success_reset_password_on_net,
pam.percent_total_create_id_count_all AS percent_total_create_id_count_all,
pam.percent_total_password_reset_success AS percent_total_password_reset_success,
pam.percent_total_success_recover_reset_username_password AS percent_total_success_recover_reset_username_password,
pam.percent_total_username_recovery_success AS percent_total_username_recovery_success,
pam.year_month AS year_month
FROM ${env:TMP_db}.net_products_agg_monthly_stage pam
LEFT JOIN ${env:TMP_db}.net_bill_pay_agg_metrics bpa
ON pam.year_month = bpa.year_month AND pam.company = bpa.company
LEFT JOIN ${env:TMP_db}.net_fed_ID_monthly fid
ON pam.year_month = fid.year_month AND pam.company = fid.company AND fid.company <> 'Total Combined'
LEFT JOIN ${env:TMP_db}.net_webmail_pv_count wmc
ON pam.year_month = wmc.year_month AND pam.company = wmc.company
LEFT JOIN ${env:TMP_db}.new_id_counts_all nid
ON pam.year_month = nid.year_month AND pam.company = nid.company
;

SELECT '*****-- End Combined net_products_agg_monthly Insert --*****' -- 14.654 seconds
;

-- End Combined net_products_agg_monthly Insert --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

