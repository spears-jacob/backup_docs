-------------------------------------------------------------------------------

--Populates the temp table net_products_agg_monthly with aggregated data for L-CHTR

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-CHTR net_products_agg_monthly Temp Table Insert --

INSERT INTO ${env:TMP_db}.net_products_agg_monthly_stage

SELECT
NULL AS hhs_logged_in,
NULL AS total_hhs,
NULL AS percent_hhs_logged_in,
NULL AS unique_hhs_mom_change,
NULL AS unique_hhs_3month_change,
--
SUM(case when message__category in('Page View') THEN 1 ELSE 0 END) AS total_page_views_count,
SUM(case when state__view__current_page__section = 'My Account' and message__category ='Page View' THEN 1 ELSE 0 END) AS my_account_page_views_count,
SUM(case when UPPER(state__view__current_page__section) = 'SUPPORT' and message__category ='Page View' THEN 1 ELSE 0 END) AS support_page_views_count,
SUM(case when message__name IN  ('Ask-Spectrum', 'Ask Spectrum') THEN 1 ELSE 0 END) AS ask_charter_requests_count,
SUM(case when message__name IN  ('Refresh') THEN 1 ELSE 0 END) AS refresh_requests_count,
SUM(case when state__view__current_page__section IN ('TV','On Demand') and message__category ='Page View' THEN 1 ELSE 0 END) AS video_page_views_count,
SUM(case when message__name IN ('Play') THEN 1 ELSE 0 END) AS video_plays_count,

--OLD New IDs Created on CHTR
SIZE(COLLECT_SET(case when message__name IN ('my-account.create-id-final.bam',
'my-account.create-id-final.btm',
'my-account.create-id-final.bam_STVA',
'my-account.create-id-final.btm_STVA') THEN visit__visit_id ELSE NULL END)) AS new_ids_charter_count,

--NEW New IDs Created
SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.bam' THEN visit__visit_id ELSE NULL END)) AS new_ids_charter_count_bam,
SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.btm' THEN visit__visit_id ELSE NULL END)) AS new_ids_charter_count_btm,
SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.nbtm' THEN visit__visit_id ELSE NULL END)) AS new_ids_charter_count_nbtm,

--NEW New IDs Attempts
(SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.bam') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.bam') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
THEN visit__visit_id ELSE NULL END))) AS attempts_create_id_count_bam,
(SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.btm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.btm') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
THEN visit__visit_id ELSE NULL END))) AS attempts_create_id_count_btm,
(SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.nbtm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.nbtm') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
THEN visit__visit_id ELSE NULL END))) AS attempts_create_id_count_nbtm,

--OLD New IDs Created and Attempts off CHTR
SIZE(COLLECT_SET(case when message__name IN ('my-account.create-id-final.nbtm',
'my-account.create-id-final.nbtm_STVA') THEN visit__visit_id ELSE NULL END)) AS new_ids_not_charter_count,
(SIZE(COLLECT_SET(case when state__view__current_page__name IN ('my-account.create-id-1.nbtm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(case when state__view__current_page__name IN ('my-account.create-id-2.nbtm')
AND message__name NOT LIKE ('.*sign\-in\-now.*') THEN visit__visit_id ELSE NULL END))) AS total_attempts_id_off_count,

--Recover Flow
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.btm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.btm') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_BTM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.btm', 'Recover-final2.btm') THEN visit__visit_id ELSE NULL END)) AS succesfull_username_recovery_BTM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.bam') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.bam') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_BAM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.bam', 'Recover-final2.bam') THEN visit__visit_id ELSE NULL END)) AS succesfull_username_recovery_BAM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.nbtm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.nbtm') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_NBTM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.nbtm', 'Recover-final2.nbtm') THEN visit__visit_id ELSE NULL END)) succesfull_username_recovery_NBTM_count,

--Reset Flow
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.btm') THEN visit__visit_id ELSE NULL END)) AS attempts_reset_password_BTM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.btm') THEN visit__visit_id ELSE NULL END)) AS successful_reset_password_BTM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.bam') THEN visit__visit_id ELSE NULL END)) AS attempts_rest_password_BAM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.bam') THEN visit__visit_id ELSE NULL END)) AS successful_reset_password_BAM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.nbtm') THEN visit__visit_id ELSE NULL END)) AS attempts_rest_password_NBTM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.nbtm') THEN visit__visit_id ELSE NULL END)) AS successful_reset_password_NBTM_count,

--Recover Reset Flow
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.btm') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_password_BTM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.btm', 'RecoverReset-final2.btm') THEN visit__visit_id ELSE NULL END)) AS successfully_recover_username_password_BTM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.bam') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_password_BAM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.bam', 'RecoverReset-final2.bam') THEN visit__visit_id ELSE NULL END)) AS successfully_recover_username_password_BAM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.nbtm') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_password_NBTM_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.nbtm', 'RecoverReset-final2.nbtm') THEN visit__visit_id ELSE NULL END)) AS successfully_recover_username_password_NBTM_count,

SIZE(COLLECT_SET(CASE WHEN message__name = 'Save'
AND state__view__current_page__elements__name =  'Billing Notifications'
THEN visit__visit_id ELSE NULL END)) AS saved_bill_notifications_count,
SIZE(COLLECT_SET(CASE WHEN message__name = 'Save'
AND state__view__current_page__elements__name = 'Appointment Reminders'
THEN visit__visit_id ELSE NULL END)) AS saved_appoint_reminders_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('reschedule-confirm') THEN visit__visit_id ELSE NULL END)) AS rescheduled_service_appoint_count,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('cancel-success-page','cancel-success-page_STVA')
THEN visit__visit_id ELSE NULL END)) AS cancelled_service_appoint_count,
SIZE(COLLECT_SET(CASE WHEN message__name = 'Save'
AND state__view__current_page__elements__name = 'Service Alerts'
THEN visit__visit_id ELSE NULL END)) AS saved_service_alerts_count,
SIZE(COLLECT_SET(CASE WHEN message__name = 'Save'
AND state__view__current_page__elements__name IN ('Email','Phone')
THEN visit__visit_id ELSE NULL END)) AS saved_contact_information_details_count,
NULL AS attempts_create_id_count,
NULL AS attempts_rest_password_count,
NULL AS successful_reset_password_count,
--
--
SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.bam' THEN visit__visit_id ELSE NULL END)) + SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.btm' THEN visit__visit_id ELSE NULL END)) AS new_ids_charter_count_on_net,
SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.nbtm' THEN visit__visit_id ELSE NULL END)) AS new_ids_charter_count_off_net,
--
((SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.bam') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.bam') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
THEN visit__visit_id ELSE NULL END)))) +
((SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.btm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.btm') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
THEN visit__visit_id ELSE NULL END)))) +
((SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.nbtm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.nbtm') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
THEN visit__visit_id ELSE NULL END)))) AS attempts_create_id_count_all,
((SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.bam') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.bam') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
THEN visit__visit_id ELSE NULL END)))) +
((SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.btm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.btm') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
THEN visit__visit_id ELSE NULL END)))) AS attempts_create_id_count_on_net,
((SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.nbtm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.nbtm') AND lower(message__name) RLIKE ('.*sign\-in\-now.*')
THEN visit__visit_id ELSE NULL END)))) AS attempts_create_id_count_off_net,
--
--
(SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.btm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.btm') THEN visit__visit_id ELSE NULL END))) +

(SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.bam') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.bam') THEN visit__visit_id ELSE NULL END))) +

(SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.nbtm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.nbtm') THEN visit__visit_id ELSE NULL END))) AS attempts_recover_username_count_all,
--
(SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.btm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.btm') THEN visit__visit_id ELSE NULL END))) +
--
(SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.bam') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.bam') THEN visit__visit_id ELSE NULL END))) AS attempts_recover_username_count_on_net,
--
(SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-1.nbtm') THEN visit__visit_id ELSE NULL END))
-
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-noID.nbtm') THEN visit__visit_id ELSE NULL END))) AS attempts_recover_username_count_off_net,
--
--
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.btm', 'Recover-final2.btm') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.bam', 'Recover-final2.bam') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.nbtm', 'Recover-final2.nbtm') THEN visit__visit_id ELSE NULL END)) AS successful_username_recovery_count_all,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.btm', 'Recover-final2.btm') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.bam', 'Recover-final2.bam') THEN visit__visit_id ELSE NULL END))  AS successful_username_recovery_count_on_net,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.nbtm', 'Recover-final2.nbtm') THEN visit__visit_id ELSE NULL END)) AS successful_username_recovery_count_off_net,
--
--
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.btm') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.bam') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.nbtm') THEN visit__visit_id ELSE NULL END)) AS attempts_reset_password_count_all,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.btm') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.bam') THEN visit__visit_id ELSE NULL END)) AS attempts_reset_password_count_on_net,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-1.nbtm') THEN visit__visit_id ELSE NULL END)) AS attempts_reset_password_count_off_net,
--
--
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.btm') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.bam') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.nbtm') THEN visit__visit_id ELSE NULL END)) AS successful_reset_password_count_all,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.btm') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.bam') THEN visit__visit_id ELSE NULL END)) AS successful_reset_password_count_on_net,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.nbtm') THEN visit__visit_id ELSE NULL END)) AS successful_reset_password_count_off_net,
--
--
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.btm') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.bam') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.nbtm') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_password_count_all,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.btm') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.bam') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_password_count_on_net,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-1.nbtm') THEN visit__visit_id ELSE NULL END)) AS attempts_recover_username_password_count_off_net,
--
--
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.btm', 'RecoverReset-final2.btm') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.bam', 'RecoverReset-final2.bam') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.nbtm', 'RecoverReset-final2.nbtm') THEN visit__visit_id ELSE NULL END)) AS successfully_recover_username_password_count_all,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.btm', 'RecoverReset-final2.btm') THEN visit__visit_id ELSE NULL END)) +
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.bam', 'RecoverReset-final2.bam') THEN visit__visit_id ELSE NULL END)) AS successfully_recover_username_password_count_on_net,
SIZE(COLLECT_SET(CASE WHEN message__name IN ('RecoverReset-final1.nbtm', 'RecoverReset-final2.nbtm') THEN visit__visit_id ELSE NULL END)) AS successfully_recover_username_password_count_off_net,
--
--
'L-CHTR' AS company,
'${env:YEAR_MONTH}' AS year_month,
COUNT(DISTINCT IF(state__view__current_page__name IN ('Test-Connection-WiFi','Test-Connection-Voice','Test-Connection-Internet','Test-Connection-InternetVoice') AND message__category = 'Page View',visit__visit_id,NULL)) AS modem_router_resets,
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
net_events
WHERE
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

SELECT '*****-- End L-CHTR net_products_agg_monthly Temp Table Insert --*****' -- 931.971 seconds
;

-- End L-CHTR net_products_agg_monthly Temp Table Insert --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
