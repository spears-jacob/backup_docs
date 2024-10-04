-------------------------------------------------------------------------------

--Combines hive table data and manual adjustment data into tableau feeder table
--Ensures that manual adjustment data "overrides" anything else with same partition
--Adds 'Total Combined' company calculations post-adjustments

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Combined Adjustments + Hive Metrics --

INSERT OVERWRITE TABLE net_products_agg_monthly_tableau
PARTITION (company,year_month,metric)

SELECT
CASE WHEN adj.change_comment IS NOT NULL THEN adj.unit ELSE hiv.unit END AS unit,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.value_type ELSE hiv.value_type END AS value_type,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.value ELSE hiv.value END AS value,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.mom_perc_chg ELSE hiv.mom_perc_chg END AS mom_perc_chg,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.mom_diff ELSE hiv.mom_diff END AS mom_diff,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prior_3_mo_perc_chg ELSE hiv.prior_3_mo_perc_chg END AS prior_3_mo_perc_chg,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prior_3_mo_diff ELSE hiv.prior_3_mo_diff END AS prior_3_mo_diff,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.ytd_avg ELSE hiv.ytd_avg END AS ytd_avg,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prev_months_max_year_month ELSE hiv.prev_months_max_year_month END AS prev_months_max_year_month,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prev_months_max_val ELSE hiv.prev_months_max_val END AS prev_months_max_val,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prev_months_min_year_month ELSE hiv.prev_months_min_year_month END AS prev_months_min_year_month,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.prev_months_min_val ELSE hiv.prev_months_min_val END AS prev_months_min_val,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.change_comment ELSE hiv.change_comment END AS change_comment,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.tableau_field ELSE dis.tableau_display END AS tableau_field,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.jira_ticket ELSE NULL END AS jira_ticket,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.company ELSE hiv.company END AS company,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.year_month ELSE hiv.year_month END AS year_month,
CASE WHEN adj.change_comment IS NOT NULL THEN adj.metric ELSE hiv.metric END AS metric
FROM
net_products_agg_monthly_pivot hiv
LEFT JOIN 
    net_products_agg_monthly_adjustment adj
    ON hiv.company = adj.company AND hiv.year_month = adj.year_month AND hiv.metric = adj.metric
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON hiv.metric = dis.hive_metric
WHERE 
hiv.year_month = '${env:YEAR_MONTH}'
;

SELECT '*****-- End Combined Adjustments + Hive Metrics Into Tableau Feed Table --*****'
;

-- End Combined Adjustments + Hive Metrics --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Total Combined Metrics --

INSERT OVERWRITE TABLE net_products_agg_monthly_tableau
PARTITION (company,year_month,metric)

SELECT
dis.unit,
dis.value_type,
value,
mom_perc_chg AS mom_perc_chg,
mom_diff AS mom_diff,
prior_3_mo_perc_chg AS prior_3_mo_perc_chg,
prior_3_mo_diff AS prior_3_mo_diff,
ytd_avg AS ytd_avg,
prev_months_max_year_month AS prev_months_max_year_month,
prev_months_max_val AS prev_months_max_val,
prev_months_min_year_month AS prev_months_min_year_month,
prev_months_min_val AS prev_months_min_val,
'total_calculations' AS change_comment,
dis.tableau_display AS tableau_field,
jira_ticket AS jira_ticket,
CASE WHEN (grouping_id & 1)!=0 THEN 'Total Combined' ELSE company END AS company,
year_month AS year_month,
metric
FROM 
(
SELECT
year_month,
company,
CASE 
    WHEN metric = 'hhs_logged_in' THEN 'subscriber'
    WHEN metric = 'total_hhs' THEN 'subscriber'
    WHEN metric = 'percent_hhs_logged_in' THEN 'percentage'
    WHEN metric = 'unique_hhs_mom_change' THEN 'percentage'
    WHEN metric = 'unique_hhs_3month_change' THEN 'percentage'
    WHEN metric = 'total_login_attempts' THEN 'instances'
    WHEN metric = 'total_login_successes' THEN 'instances'
    WHEN metric = 'percent_login_success' THEN 'percentage'
    WHEN metric = 'total_page_views_count' THEN 'page_views'
    WHEN metric = 'webmail_page_views_count' THEN 'instances'
    WHEN metric = 'my_account_page_views_count' THEN 'instances'
    WHEN metric = 'support_page_views_count' THEN 'page_views'
    WHEN metric = 'view_online_statement_count' THEN 'visits'
    WHEN metric = 'ask_charter_requests_count' THEN 'visits'
    WHEN metric = 'refresh_requests_count' THEN 'visits'
    WHEN metric = 'video_page_views_count' THEN 'page_views'
    WHEN metric = 'video_plays_count' THEN 'instances'
    WHEN metric = 'new_ids_charter_count' THEN 'visits'
    WHEN metric = 'new_ids_charter_count_bam' THEN 'visits'
    WHEN metric = 'new_ids_charter_count_btm' THEN 'visits'
    WHEN metric = 'new_ids_charter_count_nbtm' THEN 'visits'
    WHEN metric = 'attempts_create_id_count_bam' THEN 'visits'
    WHEN metric = 'attempts_create_id_count_btm' THEN 'visits'
    WHEN metric = 'attempts_create_id_count_nbtm' THEN 'visits'
    WHEN metric = 'new_ids_not_charter_count' THEN 'visits'
    WHEN metric = 'total_attempts_id_off_count' THEN 'visits'
    WHEN metric = 'attempts_recover_username_btm_count' THEN 'visits'
    WHEN metric = 'succesfull_username_recovery_btm_count' THEN 'visits'
    WHEN metric = 'attempts_recover_username_bam_count' THEN 'visits'
    WHEN metric = 'succesfull_username_recovery_bam_count' THEN 'visits'
    WHEN metric = 'attempts_recover_username_nbtm_count' THEN 'visits'
    WHEN metric = 'succesfull_username_recovery_nbtm_count' THEN 'visits'
    WHEN metric = 'attempts_reset_password_btm_count' THEN 'visits'
    WHEN metric = 'successful_reset_password_btm_count' THEN 'visits'
    WHEN metric = 'attempts_reset_password_bam_count' THEN 'visits'
    WHEN metric = 'successful_reset_password_bam_count' THEN 'visits'
    WHEN metric = 'attempts_reset_password_nbtm_count' THEN 'visits'
    WHEN metric = 'successful_reset_password_nbtm_count' THEN 'visits'
    WHEN metric = 'attempts_recover_username_password_btm_count' THEN 'visits'
    WHEN metric = 'successfully_recover_username_password_btm_count' THEN 'visits'
    WHEN metric = 'attempts_recover_username_password_bam_count' THEN 'visits'
    WHEN metric = 'successfully_recover_username_password_bam_count' THEN 'visits'
    WHEN metric = 'attempts_recover_username_password_nbtm_count' THEN 'visits'
    WHEN metric = 'successfully_recover_username_password_nbtm_count' THEN 'visits'
    WHEN metric = 'one_time_payment_count' THEN 'visits'
    WHEN metric = 'one_time_payments_confirm_count' THEN 'visits'
    WHEN metric = 'setup_autopay_count' THEN 'visits'
    WHEN metric = 'successful_autopay_confirm_count' THEN 'visits'
    WHEN metric = 'saved_bill_notifications_count' THEN 'visits'
    WHEN metric = 'saved_appoint_reminders_count' THEN 'visits'
    WHEN metric = 'rescheduled_service_appoint_count' THEN 'visits'
    WHEN metric = 'cancelled_service_appoint_count' THEN 'visits'
    WHEN metric = 'saved_service_alerts_count' THEN 'visits'
    WHEN metric = 'saved_contact_information_details_count' THEN 'visits'
    WHEN metric = 'attempts_create_id_count' THEN 'visits'
    WHEN metric = 'attempts_reset_password_count' THEN 'visits'
    WHEN metric = 'successful_reset_password_count' THEN 'visits'
    WHEN metric = 'new_ids_charter_count_all' THEN 'visits'
    WHEN metric = 'new_ids_charter_count_on_net' THEN 'visits'
    WHEN metric = 'new_ids_charter_count_off_net' THEN 'visits'
    WHEN metric = 'attempts_create_id_count_all' THEN 'visits'
    WHEN metric = 'attempts_create_id_count_on_net' THEN 'visits'
    WHEN metric = 'attempts_create_id_count_off_net' THEN 'visits'
    WHEN metric = 'attempts_recover_username_count_all' THEN 'visits'
    WHEN metric = 'attempts_recover_username_count_on_net' THEN 'visits'
    WHEN metric = 'attempts_recover_username_count_off_net' THEN 'visits'
    WHEN metric = 'successful_username_recovery_count_all' THEN 'visits'
    WHEN metric = 'successful_username_recovery_count_on_net' THEN 'visits'
    WHEN metric = 'successful_username_recovery_count_off_net' THEN 'visits'
    WHEN metric = 'attempts_reset_password_count_all' THEN 'visits'
    WHEN metric = 'attempts_reset_password_count_on_net' THEN 'visits'
    WHEN metric = 'attempts_reset_password_count_off_net' THEN 'visits'
    WHEN metric = 'successful_reset_password_count_all' THEN 'visits'
    WHEN metric = 'successful_reset_password_count_on_net' THEN 'visits'
    WHEN metric = 'successful_reset_password_count_off_net' THEN 'visits'
    WHEN metric = 'attempts_recover_username_password_count_all' THEN 'visits'
    WHEN metric = 'attempts_recover_username_password_count_on_net' THEN 'visits'
    WHEN metric = 'attempts_recover_username_password_count_off_net' THEN 'visits'
    WHEN metric = 'successfully_recover_username_password_count_all' THEN 'visits'
    WHEN metric = 'successfully_recover_username_password_count_on_net' THEN 'visits'
    WHEN metric = 'successfully_recover_username_password_count_off_net' THEN 'visits'
    ELSE unit END AS unit,
CASE
    WHEN metric = 'hhs_logged_in' THEN 'dec'
    WHEN metric = 'total_hhs' THEN 'dec'
    WHEN metric = 'percent_hhs_logged_in' THEN 'perc'
    WHEN metric = 'unique_hhs_mom_change' THEN 'perc'
    WHEN metric = 'unique_hhs_3month_change' THEN 'perc'
    WHEN metric = 'total_login_attempts' THEN 'dec'
    WHEN metric = 'total_login_successes' THEN 'dec'
    WHEN metric = 'percent_login_success' THEN 'perc'
    WHEN metric = 'total_page_views_count' THEN 'dec'
    WHEN metric = 'webmail_page_views_count' THEN 'dec'
    WHEN metric = 'my_account_page_views_count' THEN 'dec'
    WHEN metric = 'support_page_views_count' THEN 'dec'
    WHEN metric = 'view_online_statement_count' THEN 'dec'
    WHEN metric = 'ask_charter_requests_count' THEN 'dec'
    WHEN metric = 'refresh_requests_count' THEN 'dec'
    WHEN metric = 'video_page_views_count' THEN 'dec'
    WHEN metric = 'video_plays_count' THEN 'dec'
    WHEN metric = 'new_ids_charter_count' THEN 'dec'
    WHEN metric = 'new_ids_charter_count_bam' THEN 'dec'
    WHEN metric = 'new_ids_charter_count_btm' THEN 'dec'
    WHEN metric = 'new_ids_charter_count_nbtm' THEN 'dec'
    WHEN metric = 'attempts_create_id_count_bam' THEN 'dec'
    WHEN metric = 'attempts_create_id_count_btm' THEN 'dec'
    WHEN metric = 'attempts_create_id_count_nbtm' THEN 'dec'
    WHEN metric = 'new_ids_not_charter_count' THEN 'dec'
    WHEN metric = 'total_attempts_id_off_count' THEN 'dec'
    WHEN metric = 'attempts_recover_username_btm_count' THEN 'dec'
    WHEN metric = 'succesfull_username_recovery_btm_count' THEN 'dec'
    WHEN metric = 'attempts_recover_username_bam_count' THEN 'dec'
    WHEN metric = 'succesfull_username_recovery_bam_count' THEN 'dec'
    WHEN metric = 'attempts_recover_username_nbtm_count' THEN 'dec'
    WHEN metric = 'succesfull_username_recovery_nbtm_count' THEN 'dec'
    WHEN metric = 'attempts_reset_password_btm_count' THEN 'dec'
    WHEN metric = 'successful_reset_password_btm_count' THEN 'dec'
    WHEN metric = 'attempts_reset_password_bam_count' THEN 'dec'
    WHEN metric = 'successful_reset_password_bam_count' THEN 'dec'
    WHEN metric = 'attempts_reset_password_nbtm_count' THEN 'dec'
    WHEN metric = 'successful_reset_password_nbtm_count' THEN 'dec'
    WHEN metric = 'attempts_recover_username_password_btm_count' THEN 'dec'
    WHEN metric = 'successfully_recover_username_password_btm_count' THEN 'dec'
    WHEN metric = 'attempts_recover_username_password_bam_count' THEN 'dec'
    WHEN metric = 'successfully_recover_username_password_bam_count' THEN 'dec'
    WHEN metric = 'attempts_recover_username_password_nbtm_count' THEN 'dec'
    WHEN metric = 'successfully_recover_username_password_nbtm_count' THEN 'dec'
    WHEN metric = 'one_time_payment_count' THEN 'dec'
    WHEN metric = 'one_time_payments_confirm_count' THEN 'dec'
    WHEN metric = 'setup_autopay_count' THEN 'dec'
    WHEN metric = 'successful_autopay_confirm_count' THEN 'dec'
    WHEN metric = 'saved_bill_notifications_count' THEN 'dec'
    WHEN metric = 'saved_appoint_reminders_count' THEN 'dec'
    WHEN metric = 'rescheduled_service_appoint_count' THEN 'dec'
    WHEN metric = 'cancelled_service_appoint_count' THEN 'dec'
    WHEN metric = 'saved_service_alerts_count' THEN 'dec'
    WHEN metric = 'saved_contact_information_details_count' THEN 'dec'
    WHEN metric = 'attempts_create_id_count' THEN 'dec'
    WHEN metric = 'attempts_reset_password_count' THEN 'dec'
    WHEN metric = 'successful_reset_password_count' THEN 'dec'
    WHEN metric = 'new_ids_charter_count_all' THEN 'dec'
    WHEN metric = 'new_ids_charter_count_on_net' THEN 'dec'
    WHEN metric = 'new_ids_charter_count_off_net' THEN 'dec'
    WHEN metric = 'attempts_create_id_count_all' THEN 'dec'
    WHEN metric = 'attempts_create_id_count_on_net' THEN 'dec'
    WHEN metric = 'attempts_create_id_count_off_net' THEN 'dec'
    WHEN metric = 'attempts_recover_username_count_all' THEN 'dec'
    WHEN metric = 'attempts_recover_username_count_on_net' THEN 'dec'
    WHEN metric = 'attempts_recover_username_count_off_net' THEN 'dec'
    WHEN metric = 'successful_username_recovery_count_all' THEN 'dec'
    WHEN metric = 'successful_username_recovery_count_on_net' THEN 'dec'
    WHEN metric = 'successful_username_recovery_count_off_net' THEN 'dec'
    WHEN metric = 'attempts_reset_password_count_all' THEN 'dec'
    WHEN metric = 'attempts_reset_password_count_on_net' THEN 'dec'
    WHEN metric = 'attempts_reset_password_count_off_net' THEN 'dec'
    WHEN metric = 'successful_reset_password_count_all' THEN 'dec'
    WHEN metric = 'successful_reset_password_count_on_net' THEN 'dec'
    WHEN metric = 'successful_reset_password_count_off_net' THEN 'dec'
    WHEN metric = 'attempts_recover_username_password_count_all' THEN 'dec'
    WHEN metric = 'attempts_recover_username_password_count_on_net' THEN 'dec'
    WHEN metric = 'attempts_recover_username_password_count_off_net' THEN 'dec'
    WHEN metric = 'successfully_recover_username_password_count_all' THEN 'dec'
    WHEN metric = 'successfully_recover_username_password_count_on_net' THEN 'dec'
    WHEN metric = 'successfully_recover_username_password_count_off_net' THEN 'dec'
    ELSE value_type END AS value_type,
SUM(IF(value IS NOT NULL,value,NULL)) AS value,
mom_perc_chg AS mom_perc_chg,
mom_diff AS mom_diff,
prior_3_mo_perc_chg AS prior_3_mo_perc_chg,
prior_3_mo_diff AS prior_3_mo_diff,
ytd_avg AS ytd_avg,
prev_months_max_year_month AS prev_months_max_year_month,
prev_months_max_val AS prev_months_max_val,
prev_months_min_year_month AS prev_months_min_year_month,
prev_months_min_val AS prev_months_min_val,
'total_calculations' AS change_comment,
tableau_field AS tableau_field,
jira_ticket AS jira_ticket,
metric,
CAST(grouping__id AS INT) AS grouping_id
FROM 
net_products_agg_monthly_tableau
WHERE year_month = '${env:YEAR_MONTH}'
    AND company <> 'Total Combined'
GROUP BY
year_month,
company,
metric,
unit,
value_type,
mom_perc_chg,
mom_diff,
prior_3_mo_perc_chg,
prior_3_mo_diff,
ytd_avg,
prev_months_max_year_month,
prev_months_max_val,
prev_months_min_year_month,
prev_months_min_val,
'total_calculations',
tableau_field,
jira_ticket
GROUPING SETS ((year_month,metric))
) met_group
LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp dis
    ON metric = dis.hive_metric
;

SELECT '*****-- End Total Combined Metrics Into Tableau Feed Table --*****' 
;

-- End net_products_agg_monthly_tableau TOTALS Insert --
-------------------------------------------------------------------------------


