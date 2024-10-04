-------------------------------------------------------------------------------

--Populates the temp table net_products_agg_monthly with aggregated data for L-BHN

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-BHN net_products_agg_monthly Temp Table Insert --

INSERT INTO ${env:TMP_db}.net_products_agg_monthly_stage

SELECT
NULL AS hhs_logged_in,
NULL AS total_hhs,
NULL AS percent_hhs_logged_in,
NULL AS unique_hhs_mom_change,
NULL AS unique_hhs_3month_change,
SUM(IF(ev.message__category = 'Page View',1,0)) AS total_page_views_count,
SUM(IF(ev.message__category = 'Page View' and ev.state__view__current_page__section = 'my services active',1,0)) AS my_account_page_views_count,
SUM(IF(ev.message__category = 'Page View' and ev.state__view__current_page__section = 'support active',1,0)) AS support_page_views_count,
NULL AS ask_charter_requests_count,
NULL AS refresh_requests_count,
NULL AS video_page_views_count,
NULL AS video_plays_count,
NULL AS new_ids_charter_count,
NULL AS new_ids_charter_count_bam,
NULL AS new_ids_charter_count_btm,
NULL AS new_ids_charter_count_nbtm,
NULL AS attempts_create_id_count_bam,
NULL AS attempts_create_id_count_btm,
NULL AS attempts_create_id_count_nbtm,
NULL AS new_ids_not_charter_count,
NULL AS total_attempts_id_off_count,
NULL AS attempts_recover_username_btm_count,
NULL AS succesfull_username_recovery_btm_count,
NULL AS attempts_recover_username_bam_count,
NULL AS succesfull_username_recovery_bam_count,
NULL AS attempts_recover_username_nbtm_count,
NULL AS succesfull_username_recovery_nbtm_count,
NULL AS attempts_reset_password_btm_count,
NULL AS successful_reset_password_btm_count,
NULL AS attempts_rest_password_bam_count,
NULL AS successful_reset_password_bam_count,
NULL AS attempts_rest_password_nbtm_count,
NULL AS successful_reset_password_nbtm_count,
NULL AS attempts_recover_username_password_btm_count,
NULL AS successfully_recover_username_password_btm_count,
NULL AS attempts_recover_username_password_bam_count,
NULL AS successfully_recover_username_password_bam_count,
NULL AS attempts_recover_username_password_nbtm_count,
NULL AS successfully_recover_username_password_nbtm_count,
--
NULL AS saved_bill_notifications_count,
NULL AS saved_appoint_reminders_count,
NULL AS rescheduled_service_appoint_count,
NULL AS cancelled_service_appoint_count,
NULL AS saved_service_alerts_count,
NULL AS saved_contact_information_details_count,
NULL AS attempts_create_id_count,
NULL AS attempts_rest_password_count,
NULL AS successful_reset_password_count,
NULL AS new_ids_charter_count_on_net,
NULL AS new_ids_charter_count_off_net,
NULL AS attempts_create_id_count_all,  -- field not used for BHN at this time
NULL AS attempts_create_id_count_on_net,
NULL AS attempts_create_id_count_off_net,
NULL AS attempts_recover_username_count_all,
NULL AS attempts_recover_username_count_on_net,
NULL AS attempts_recover_username_count_off_net,
NULL AS successful_username_recovery_count_all,
NULL AS successful_username_recovery_count_on_net,
NULL AS successful_username_recovery_count_off_net,
NULL AS attempts_reset_password_count_all,
NULL AS attempts_reset_password_count_on_net,
NULL AS attempts_reset_password_count_off_net,
NULL AS successful_reset_password_count_all,
NULL AS successful_reset_password_count_on_net,
NULL AS successful_reset_password_count_off_net,
NULL AS attempts_recover_username_password_count_all,
NULL AS attempts_recover_username_password_count_on_net,
NULL AS attempts_recover_username_password_count_off_net,
NULL AS successfully_recover_username_password_count_all,
NULL AS successfully_recover_username_password_count_on_net,
NULL AS successfully_recover_username_password_count_off_net,
'L-BHN' AS company,
'${env:YEAR_MONTH}' AS year_month,
NULL AS modem_router_resets,
NULL AS percent_auto_pay_success,
NULL AS percent_id_recovery_success_off_chtr_network,
NULL AS percent_id_recovery_success_on_chtr_network,
NULL AS percent_new_ids_charter_count_on_net,
NULL AS percent_one_time_payment_success,
NULL AS percent_password_reset_success_off_chtr_network,
NULL AS percent_success_new_ids_charter_off_net,
NULL AS percent_success_recover_reset_username_password_off_net,
NULL AS percent_success_recover_reset_username_password_on_net,
NULL AS percent_success_reset_password_on_net,
NULL AS percent_total_create_id_count_all,
NULL AS percent_total_password_reset_success,
NULL AS percent_total_success_recover_reset_username_password,
NULL AS percent_total_username_recovery_success
FROM
bhn_residential_events ev
WHERE
    (ev.partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
    AND epoch_converter(CAST(ev.message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

SELECT '*****-- End L-BHN net_products_agg_monthly Temp Table Insert --*****' -- 176.414 seconds
;

-- End L-BHN net_products_agg_monthly Temp Table Insert --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
