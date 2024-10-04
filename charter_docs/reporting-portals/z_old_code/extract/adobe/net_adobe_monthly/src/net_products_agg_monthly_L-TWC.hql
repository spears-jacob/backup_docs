-------------------------------------------------------------------------------

--Populates the temp table net_products_agg_monthly with aggregated data for L-TWC

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-TWC net_products_agg_monthly Temp Table Insert --

INSERT INTO ${env:TMP_db}.net_products_agg_monthly_stage

SELECT
NULL AS hhs_logged_in,
NULL AS total_hhs,
NULL AS percent_hhs_logged_in,
NULL AS unique_hhs_mom_change,
NULL AS unique_hhs_3month_change,
SUM(IF(message__category = 'Page View',1,0)) AS total_page_views_count,
SUM(IF(message__category = 'Page View' and state__view__current_page__section = 'services',1,0)) AS my_account_page_views_count,
SUM(IF((LOWER(state__view__current_page__page_name)  RLIKE '.*support.*') AND (message__category = 'Page View') AND (LOWER(state__view__current_page__section) RLIKE '.*support.*'), 1, 0)) AS support_page_views_count,
SIZE(COLLECT_SET(IF(LOWER(state__search__text) like '%askamy%',visit__visit_id,NULL))) AS ask_charter_requests_count,
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section RLIKE '^ats:troubleshoot:tv.*confirmation$',visit__visit_id,NULL))) AS refresh_requests_count,
--SIZE(COLLECT_SET(IF(state__view__current_page__sub_section like 'reauthorize',visit__visit_id,NULL))) + SIZE(COLLECT_SET(IF(state__view__current_page__sub_section like 'reboot',visit__visit_id,NULL))) AS refresh_requests_count,
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
NULL AS attempts_reset_password_bam_count,
NULL AS successful_reset_password_bam_count,
NULL AS attempts_reset_password_nbtm_count,
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
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section = 'services : my services : appointment manager : reschedule submitted',visit__visit_id,NULL))) AS rescheduled_service_appoint_count,
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section = 'my services > appointment manager > cancel submitted',visit__visit_id,NULL))) AS cancelled_service_appoint_count,
NULL AS saved_service_alerts_count,
NULL AS saved_contact_information_details_count,
NULL AS attempts_create_id_count,
NULL AS attempts_reset_password_count,
NULL AS successful_reset_password_count,
SUM(IF(array_contains(message__feature__name,'Custom Event 5') and visit__connection__network_status='cla 3.0:in home',1,0)) AS new_ids_charter_count_on_net,
SUM(IF(array_contains(message__feature__name,'Custom Event 5') and visit__connection__network_status='cla 3.0:out of home',1,0)) AS new_ids_charter_count_off_net,
SUM(IF(array_contains(message__feature__name,'Custom Event 1'),1,0)) AS attempts_create_id_count_all,
SUM(IF(array_contains(message__feature__name,'Custom Event 1') and visit__connection__network_status='cla 3.0:in home',1,0)) AS attempts_create_id_count_on_net,
SUM(IF(array_contains(message__feature__name,'Custom Event 1') and visit__connection__network_status='cla 3.0:out of home',1,0)) AS attempts_create_id_count_off_net,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > retrieve username > lookup account',visit__visit_id,NULL))) AS attempts_recover_username_count_all,
NULL AS attempts_recover_username_count_on_net,
NULL AS attempts_recover_username_count_off_net,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > retrieve username > username displayed' OR state__view__current_page__page_name = 'cla > retrieve username > email sent',visit__visit_id,NULL))) AS successful_username_recovery_count_all,
NULL AS successful_username_recovery_count_on_net,
NULL AS successful_username_recovery_count_off_net,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > reset password > step 1',visit__visit_id,NULL))) AS attempts_reset_password_count_all,
NULL AS attempts_reset_password_count_on_net,
NULL AS attempts_reset_password_count_off_net,
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > reset password > change password confirmation',visit__visit_id,NULL))) AS successful_reset_password_count_all,
NULL AS successful_reset_password_count_on_net,
NULL AS successful_reset_password_count_off_net,
NULL AS attempts_recover_username_password_count_all,
NULL AS attempts_recover_username_password_count_on_net,
NULL AS attempts_recover_username_password_count_off_net,
NULL AS successfully_recover_username_password_count_all,
NULL AS successfully_recover_username_password_count_on_net,
NULL AS successfully_recover_username_password_count_off_net,
'L-TWC' AS company,
'${env:YEAR_MONTH}' AS year_month,
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section RLIKE '^ats:troubleshoot:homephone.*confirmation$' OR state__view__current_page__sub_section RLIKE '^ats:troubleshoot:internet.*confirmation$',visit__visit_id,NULL))) AS modem_router_resets,
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
twc_residential_global_events
WHERE
    (partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
    AND epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
GROUP BY
date_yearmonth(epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver'))
;

SELECT '*****-- End L-TWC net_products_agg_monthly Temp Table Insert --*****' -- 480.997 seconds
;

-- End L-TWC net_products_agg_monthly Temp Table Insert --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
