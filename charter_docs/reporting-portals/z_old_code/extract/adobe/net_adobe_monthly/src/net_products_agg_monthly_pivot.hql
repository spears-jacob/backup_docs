-------------------------------------------------------------------------------

--Takes all hive table data and pivots into tableau feeder table format

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT}
;

-------------------------------------------------------------------------------

INSERT OVERWRITE TABLE net_products_agg_monthly_pivot 
PARTITION (company,year_month,metric)

SELECT
CASE
    WHEN metric_name = 'hhs_logged_in' THEN 'subscriber'
    WHEN metric_name = 'total_hhs' THEN 'subscriber'
    WHEN metric_name = 'percent_hhs_logged_in' THEN 'percentage'
    WHEN metric_name = 'unique_hhs_mom_change' THEN 'percentage'
    WHEN metric_name = 'unique_hhs_3month_change' THEN 'percentage'
    WHEN metric_name = 'total_login_attempts' THEN 'instances'
    WHEN metric_name = 'total_login_successes' THEN 'instances'
    WHEN metric_name = 'percent_login_success' THEN 'percentage'
    WHEN metric_name = 'total_page_views_count' THEN 'page_views'
    WHEN metric_name = 'webmail_page_views_count' THEN 'instances'
    WHEN metric_name = 'my_account_page_views_count' THEN 'instances'
    WHEN metric_name = 'support_page_views_count' THEN 'page_views'
    WHEN metric_name = 'view_online_statement_count' THEN 'visits'
    WHEN metric_name = 'ask_charter_requests_count' THEN 'visits'
    WHEN metric_name = 'refresh_requests_count' THEN 'visits'
    WHEN metric_name = 'video_page_views_count' THEN 'page_views'
    WHEN metric_name = 'video_plays_count' THEN 'instances'
    WHEN metric_name = 'new_ids_charter_count' THEN 'visits'
    WHEN metric_name = 'new_ids_charter_count_bam' THEN 'visits'
    WHEN metric_name = 'new_ids_charter_count_btm' THEN 'visits'
    WHEN metric_name = 'new_ids_charter_count_nbtm' THEN 'visits'
    WHEN metric_name = 'attempts_create_id_count_bam' THEN 'visits'
    WHEN metric_name = 'attempts_create_id_count_btm' THEN 'visits'
    WHEN metric_name = 'attempts_create_id_count_nbtm' THEN 'visits'
    WHEN metric_name = 'new_ids_not_charter_count' THEN 'visits'
    WHEN metric_name = 'total_attempts_id_off_count' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_btm_count' THEN 'visits'
    WHEN metric_name = 'succesfull_username_recovery_btm_count' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_bam_count' THEN 'visits'
    WHEN metric_name = 'succesfull_username_recovery_bam_count' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_nbtm_count' THEN 'visits'
    WHEN metric_name = 'succesfull_username_recovery_nbtm_count' THEN 'visits'
    WHEN metric_name = 'attempts_reset_password_btm_count' THEN 'visits'
    WHEN metric_name = 'successful_reset_password_btm_count' THEN 'visits'
    WHEN metric_name = 'attempts_reset_password_bam_count' THEN 'visits'
    WHEN metric_name = 'successful_reset_password_bam_count' THEN 'visits'
    WHEN metric_name = 'attempts_reset_password_nbtm_count' THEN 'visits'
    WHEN metric_name = 'successful_reset_password_nbtm_count' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_password_btm_count' THEN 'visits'
    WHEN metric_name = 'successfully_recover_username_password_btm_count' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_password_bam_count' THEN 'visits'
    WHEN metric_name = 'successfully_recover_username_password_bam_count' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_password_nbtm_count' THEN 'visits'
    WHEN metric_name = 'successfully_recover_username_password_nbtm_count' THEN 'visits'
    WHEN metric_name = 'one_time_payment_count' THEN 'visits'
    WHEN metric_name = 'one_time_payments_confirm_count' THEN 'visits'
    WHEN metric_name = 'setup_autopay_count' THEN 'visits'
    WHEN metric_name = 'successful_autopay_confirm_count' THEN 'visits'
    WHEN metric_name = 'saved_bill_notifications_count' THEN 'visits'
    WHEN metric_name = 'saved_appoint_reminders_count' THEN 'visits'
    WHEN metric_name = 'rescheduled_service_appoint_count' THEN 'visits'
    WHEN metric_name = 'cancelled_service_appoint_count' THEN 'visits'
    WHEN metric_name = 'saved_service_alerts_count' THEN 'visits'
    WHEN metric_name = 'saved_contact_information_details_count' THEN 'visits'
    WHEN metric_name = 'attempts_create_id_count' THEN 'visits'
    WHEN metric_name = 'attempts_reset_password_count' THEN 'visits'
    WHEN metric_name = 'successful_reset_password_count' THEN 'visits'
    WHEN metric_name = 'new_ids_charter_count_all' THEN 'visits'
    WHEN metric_name = 'new_ids_charter_count_on_net' THEN 'visits'
    WHEN metric_name = 'new_ids_charter_count_off_net' THEN 'visits'
    WHEN metric_name = 'attempts_create_id_count_all' THEN 'visits'
    WHEN metric_name = 'attempts_create_id_count_on_net' THEN 'visits'
    WHEN metric_name = 'attempts_create_id_count_off_net' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_count_all' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_count_on_net' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_count_off_net' THEN 'visits'
    WHEN metric_name = 'successful_username_recovery_count_all' THEN 'visits'
    WHEN metric_name = 'successful_username_recovery_count_on_net' THEN 'visits'
    WHEN metric_name = 'successful_username_recovery_count_off_net' THEN 'visits'
    WHEN metric_name = 'attempts_reset_password_count_all' THEN 'visits'
    WHEN metric_name = 'attempts_reset_password_count_on_net' THEN 'visits'
    WHEN metric_name = 'attempts_reset_password_count_off_net' THEN 'visits'
    WHEN metric_name = 'successful_reset_password_count_all' THEN 'visits'
    WHEN metric_name = 'successful_reset_password_count_on_net' THEN 'visits'
    WHEN metric_name = 'successful_reset_password_count_off_net' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_password_count_all' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_password_count_on_net' THEN 'visits'
    WHEN metric_name = 'attempts_recover_username_password_count_off_net' THEN 'visits'
    WHEN metric_name = 'successfully_recover_username_password_count_all' THEN 'visits'
    WHEN metric_name = 'successfully_recover_username_password_count_on_net' THEN 'visits'
    WHEN metric_name = 'successfully_recover_username_password_count_off_net' THEN 'visits'
    WHEN metric_name = 'modem_router_resets' THEN 'visits'
    WHEN metric_name = 'percent_auto_pay_success' THEN 'percentage'
    WHEN metric_name = 'percent_id_recovery_success_off_chtr_network' THEN 'percentage'
    WHEN metric_name = 'percent_id_recovery_success_on_chtr_network' THEN 'percentage'
    WHEN metric_name = 'percent_new_ids_charter_count_on_net' THEN 'percentage'
    WHEN metric_name = 'percent_one_time_payment_success' THEN 'percentage'
    WHEN metric_name = 'percent_password_reset_success_off_chtr_network' THEN 'percentage'
    WHEN metric_name = 'percent_success_new_ids_charter_off_net' THEN 'percentage'
    WHEN metric_name = 'percent_success_recover_reset_username_password_off_net' THEN 'percentage'
    WHEN metric_name = 'percent_success_recover_reset_username_password_on_net' THEN 'percentage'
    WHEN metric_name = 'percent_success_reset_password_on_net' THEN 'percentage'
    WHEN metric_name = 'percent_total_create_id_count_all' THEN 'percentage'
    WHEN metric_name = 'percent_total_password_reset_success' THEN 'percentage'
    WHEN metric_name = 'percent_total_success_recover_reset_username_password' THEN 'percentage'
    WHEN metric_name = 'percent_total_username_recovery_success' THEN 'percentage'
    ELSE unit END AS unit,
CASE
    WHEN metric_name = 'hhs_logged_in' THEN 'dec'
    WHEN metric_name = 'total_hhs' THEN 'dec'
    WHEN metric_name = 'percent_hhs_logged_in' THEN 'perc'
    WHEN metric_name = 'unique_hhs_mom_change' THEN 'perc'
    WHEN metric_name = 'unique_hhs_3month_change' THEN 'perc'
    WHEN metric_name = 'total_login_attempts' THEN 'dec'
    WHEN metric_name = 'total_login_successes' THEN 'dec'
    WHEN metric_name = 'percent_login_success' THEN 'perc'
    WHEN metric_name = 'total_page_views_count' THEN 'dec'
    WHEN metric_name = 'webmail_page_views_count' THEN 'dec'
    WHEN metric_name = 'my_account_page_views_count' THEN 'dec'
    WHEN metric_name = 'support_page_views_count' THEN 'dec'
    WHEN metric_name = 'view_online_statement_count' THEN 'dec'
    WHEN metric_name = 'ask_charter_requests_count' THEN 'dec'
    WHEN metric_name = 'refresh_requests_count' THEN 'dec'
    WHEN metric_name = 'video_page_views_count' THEN 'dec'
    WHEN metric_name = 'video_plays_count' THEN 'dec'
    WHEN metric_name = 'new_ids_charter_count' THEN 'dec'
    WHEN metric_name = 'new_ids_charter_count_bam' THEN 'dec'
    WHEN metric_name = 'new_ids_charter_count_btm' THEN 'dec'
    WHEN metric_name = 'new_ids_charter_count_nbtm' THEN 'dec'
    WHEN metric_name = 'attempts_create_id_count_bam' THEN 'dec'
    WHEN metric_name = 'attempts_create_id_count_btm' THEN 'dec'
    WHEN metric_name = 'attempts_create_id_count_nbtm' THEN 'dec'
    WHEN metric_name = 'new_ids_not_charter_count' THEN 'dec'
    WHEN metric_name = 'total_attempts_id_off_count' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_btm_count' THEN 'dec'
    WHEN metric_name = 'succesfull_username_recovery_btm_count' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_bam_count' THEN 'dec'
    WHEN metric_name = 'succesfull_username_recovery_bam_count' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_nbtm_count' THEN 'dec'
    WHEN metric_name = 'succesfull_username_recovery_nbtm_count' THEN 'dec'
    WHEN metric_name = 'attempts_reset_password_btm_count' THEN 'dec'
    WHEN metric_name = 'successful_reset_password_btm_count' THEN 'dec'
    WHEN metric_name = 'attempts_reset_password_bam_count' THEN 'dec'
    WHEN metric_name = 'successful_reset_password_bam_count' THEN 'dec'
    WHEN metric_name = 'attempts_reset_password_nbtm_count' THEN 'dec'
    WHEN metric_name = 'successful_reset_password_nbtm_count' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_password_btm_count' THEN 'dec'
    WHEN metric_name = 'successfully_recover_username_password_btm_count' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_password_bam_count' THEN 'dec'
    WHEN metric_name = 'successfully_recover_username_password_bam_count' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_password_nbtm_count' THEN 'dec'
    WHEN metric_name = 'successfully_recover_username_password_nbtm_count' THEN 'dec'
    WHEN metric_name = 'one_time_payment_count' THEN 'dec'
    WHEN metric_name = 'one_time_payments_confirm_count' THEN 'dec'
    WHEN metric_name = 'setup_autopay_count' THEN 'dec'
    WHEN metric_name = 'successful_autopay_confirm_count' THEN 'dec'
    WHEN metric_name = 'saved_bill_notifications_count' THEN 'dec'
    WHEN metric_name = 'saved_appoint_reminders_count' THEN 'dec'
    WHEN metric_name = 'rescheduled_service_appoint_count' THEN 'dec'
    WHEN metric_name = 'cancelled_service_appoint_count' THEN 'dec'
    WHEN metric_name = 'saved_service_alerts_count' THEN 'dec'
    WHEN metric_name = 'saved_contact_information_details_count' THEN 'dec'
    WHEN metric_name = 'attempts_create_id_count' THEN 'dec'
    WHEN metric_name = 'attempts_reset_password_count' THEN 'dec'
    WHEN metric_name = 'successful_reset_password_count' THEN 'dec'
    WHEN metric_name = 'new_ids_charter_count_all' THEN 'dec'
    WHEN metric_name = 'new_ids_charter_count_on_net' THEN 'dec'
    WHEN metric_name = 'new_ids_charter_count_off_net' THEN 'dec'
    WHEN metric_name = 'attempts_create_id_count_all' THEN 'dec'
    WHEN metric_name = 'attempts_create_id_count_on_net' THEN 'dec'
    WHEN metric_name = 'attempts_create_id_count_off_net' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_count_all' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_count_on_net' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_count_off_net' THEN 'dec'
    WHEN metric_name = 'successful_username_recovery_count_all' THEN 'dec'
    WHEN metric_name = 'successful_username_recovery_count_on_net' THEN 'dec'
    WHEN metric_name = 'successful_username_recovery_count_off_net' THEN 'dec'
    WHEN metric_name = 'attempts_reset_password_count_all' THEN 'dec'
    WHEN metric_name = 'attempts_reset_password_count_on_net' THEN 'dec'
    WHEN metric_name = 'attempts_reset_password_count_off_net' THEN 'dec'
    WHEN metric_name = 'successful_reset_password_count_all' THEN 'dec'
    WHEN metric_name = 'successful_reset_password_count_on_net' THEN 'dec'
    WHEN metric_name = 'successful_reset_password_count_off_net' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_password_count_all' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_password_count_on_net' THEN 'dec'
    WHEN metric_name = 'attempts_recover_username_password_count_off_net' THEN 'dec'
    WHEN metric_name = 'successfully_recover_username_password_count_all' THEN 'dec'
    WHEN metric_name = 'successfully_recover_username_password_count_on_net' THEN 'dec'
    WHEN metric_name = 'successfully_recover_username_password_count_off_net' THEN 'dec'
    WHEN metric_name = 'modem_router_resets' THEN 'dec'
    WHEN metric_name = 'percent_auto_pay_success' THEN 'perc'
    WHEN metric_name = 'percent_id_recovery_success_off_chtr_network' THEN 'perc'
    WHEN metric_name = 'percent_id_recovery_success_on_chtr_network' THEN 'perc'
    WHEN metric_name = 'percent_new_ids_charter_count_on_net' THEN 'perc'
    WHEN metric_name = 'percent_one_time_payment_success' THEN 'perc'
    WHEN metric_name = 'percent_password_reset_success_off_chtr_network' THEN 'perc'
    WHEN metric_name = 'percent_success_new_ids_charter_off_net' THEN 'perc'
    WHEN metric_name = 'percent_success_recover_reset_username_password_off_net' THEN 'perc'
    WHEN metric_name = 'percent_success_recover_reset_username_password_on_net' THEN 'perc'
    WHEN metric_name = 'percent_success_reset_password_on_net' THEN 'perc'
    WHEN metric_name = 'percent_total_create_id_count_all' THEN 'perc'
    WHEN metric_name = 'percent_total_password_reset_success' THEN 'perc'
    WHEN metric_name = 'percent_total_success_recover_reset_username_password' THEN 'perc'
    WHEN metric_name = 'percent_total_username_recovery_success' THEN 'perc'
    ELSE value_type END AS value_type,
metric_value AS value,
NULL AS mom_perc_chg,
NULL AS mom_diff,
NULL AS prior_3_mo_perc_chg,
NULL AS prior_3_mo_diff,
NULL AS ytd_avg,
NULL AS prev_months_max_year_month,
NULL AS prev_months_max_val,
NULL AS prev_months_min_year_month,
NULL AS prev_months_min_val,
'hive_table' AS change_comment,
company AS company,
year_month AS year_month,
metric_name AS metric
FROM
    (SELECT 
    company,
    year_month,
    NULL AS unit,
    NULL AS value_type,
    MAP 
        (
        'hhs_logged_in',hhs_logged_in,
        'total_hhs',total_hhs,
        'percent_hhs_logged_in',percent_hhs_logged_in,
        'unique_hhs_mom_change',unique_hhs_mom_change,
        'unique_hhs_3month_change',unique_hhs_3month_change,
        'total_login_attempts',total_login_attempts,
        'total_login_successes',total_login_successes,
        'percent_login_success',percent_login_success,
        'total_page_views_count',total_page_views_count,
        'webmail_page_views_count',webmail_page_views_count,
        'my_account_page_views_count',my_account_page_views_count,
        'support_page_views_count',support_page_views_count,
        'view_online_statement_count',view_online_statement_count,
        'ask_charter_requests_count',ask_charter_requests_count,
        'refresh_requests_count',refresh_requests_count,
        'video_page_views_count',video_page_views_count,
        'video_plays_count',video_plays_count,
        'new_ids_charter_count',new_ids_charter_count,
        'new_ids_charter_count_bam',new_ids_charter_count_bam,
        'new_ids_charter_count_btm',new_ids_charter_count_btm,
        'new_ids_charter_count_nbtm',new_ids_charter_count_nbtm,
        'attempts_create_id_count_bam',attempts_create_id_count_bam,
        'attempts_create_id_count_btm',attempts_create_id_count_btm,
        'attempts_create_id_count_nbtm',attempts_create_id_count_nbtm,
        'new_ids_not_charter_count',new_ids_not_charter_count,
        'total_attempts_id_off_count',total_attempts_id_off_count,
        'attempts_recover_username_btm_count',attempts_recover_username_btm_count,
        'succesfull_username_recovery_btm_count',succesfull_username_recovery_btm_count,
        'attempts_recover_username_bam_count',attempts_recover_username_bam_count,
        'succesfull_username_recovery_bam_count',succesfull_username_recovery_bam_count,
        'attempts_recover_username_nbtm_count',attempts_recover_username_nbtm_count,
        'succesfull_username_recovery_nbtm_count',succesfull_username_recovery_nbtm_count,
        'attempts_reset_password_btm_count',attempts_reset_password_btm_count,
        'successful_reset_password_btm_count',successful_reset_password_btm_count,
        'attempts_reset_password_bam_count',attempts_reset_password_bam_count,
        'successful_reset_password_bam_count',successful_reset_password_bam_count,
        'attempts_reset_password_nbtm_count',attempts_reset_password_nbtm_count,
        'successful_reset_password_nbtm_count',successful_reset_password_nbtm_count,
        'attempts_recover_username_password_btm_count',attempts_recover_username_password_btm_count,
        'successfully_recover_username_password_btm_count',successfully_recover_username_password_btm_count,
        'attempts_recover_username_password_bam_count',attempts_recover_username_password_bam_count,
        'successfully_recover_username_password_bam_count',successfully_recover_username_password_bam_count,
        'attempts_recover_username_password_nbtm_count',attempts_recover_username_password_nbtm_count,
        'successfully_recover_username_password_nbtm_count',successfully_recover_username_password_nbtm_count,
        'one_time_payment_count',one_time_payment_count,
        'one_time_payments_confirm_count',one_time_payments_confirm_count,
        'setup_autopay_count',setup_autopay_count,
        'successful_autopay_confirm_count',successful_autopay_confirm_count,
        'saved_bill_notifications_count',saved_bill_notifications_count,
        'saved_appoint_reminders_count',saved_appoint_reminders_count,
        'rescheduled_service_appoint_count',rescheduled_service_appoint_count,
        'cancelled_service_appoint_count',cancelled_service_appoint_count,
        'saved_service_alerts_count',saved_service_alerts_count,
        'saved_contact_information_details_count',saved_contact_information_details_count,
        'attempts_create_id_count',attempts_create_id_count,
        'attempts_reset_password_count',attempts_reset_password_count,
        'successful_reset_password_count',successful_reset_password_count,
        'new_ids_charter_count_all',new_ids_charter_count_all,
        'new_ids_charter_count_on_net',new_ids_charter_count_on_net,
        'new_ids_charter_count_off_net',new_ids_charter_count_off_net,
        'attempts_create_id_count_all',attempts_create_id_count_all,
        'attempts_create_id_count_on_net',attempts_create_id_count_on_net,
        'attempts_create_id_count_off_net',attempts_create_id_count_off_net,
        'attempts_recover_username_count_all',attempts_recover_username_count_all,
        'attempts_recover_username_count_on_net',attempts_recover_username_count_on_net,
        'attempts_recover_username_count_off_net',attempts_recover_username_count_off_net,
        'successful_username_recovery_count_all',successful_username_recovery_count_all,
        'successful_username_recovery_count_on_net',successful_username_recovery_count_on_net,
        'successful_username_recovery_count_off_net',successful_username_recovery_count_off_net,
        'attempts_reset_password_count_all',attempts_reset_password_count_all,
        'attempts_reset_password_count_on_net',attempts_reset_password_count_on_net,
        'attempts_reset_password_count_off_net',attempts_reset_password_count_off_net,
        'successful_reset_password_count_all',successful_reset_password_count_all,
        'successful_reset_password_count_on_net',successful_reset_password_count_on_net,
        'successful_reset_password_count_off_net',successful_reset_password_count_off_net,
        'attempts_recover_username_password_count_all',attempts_recover_username_password_count_all,
        'attempts_recover_username_password_count_on_net',attempts_recover_username_password_count_on_net,
        'attempts_recover_username_password_count_off_net',attempts_recover_username_password_count_off_net,
        'successfully_recover_username_password_count_all',successfully_recover_username_password_count_all,
        'successfully_recover_username_password_count_on_net',successfully_recover_username_password_count_on_net,
        'successfully_recover_username_password_count_off_net',successfully_recover_username_password_count_off_net,
        'modem_router_resets',modem_router_resets,
        'percent_auto_pay_success',percent_auto_pay_success,
        'percent_id_recovery_success_off_chtr_network',percent_id_recovery_success_off_chtr_network,
        'percent_id_recovery_success_on_chtr_network',percent_id_recovery_success_on_chtr_network,
        'percent_new_ids_charter_count_on_net',percent_new_ids_charter_count_on_net,
        'percent_one_time_payment_success',percent_one_time_payment_success,
        'percent_password_reset_success_off_chtr_network',percent_password_reset_success_off_chtr_network,
        'percent_success_new_ids_charter_off_net',percent_success_new_ids_charter_off_net,
        'percent_success_recover_reset_username_password_off_net',percent_success_recover_reset_username_password_off_net,
        'percent_success_recover_reset_username_password_on_net',percent_success_recover_reset_username_password_on_net,
        'percent_success_reset_password_on_net',percent_success_reset_password_on_net,
        'percent_total_create_id_count_all',percent_total_create_id_count_all,
        'percent_total_password_reset_success',percent_total_password_reset_success,
        'percent_total_success_recover_reset_username_password',percent_total_success_recover_reset_username_password,
        'percent_total_username_recovery_success',percent_total_username_recovery_success
        ) met_values
    FROM net_products_agg_monthly
    WHERE year_month = '${env:YEAR_MONTH}'
    AND company <> 'Total Combined'
    ) mv 
LATERAL VIEW EXPLODE(met_values) xyz AS metric_name,metric_value


