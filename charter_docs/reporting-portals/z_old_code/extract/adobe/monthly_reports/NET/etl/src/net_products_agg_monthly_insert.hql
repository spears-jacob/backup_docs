USE ${env:ENVIRONMENT};

INSERT INTO net_products_agg_monthly PARTITION (year_month)

SELECT
sum(case when message__category in('Page View') then 1 else 0 end)  as total_page_views_count,
sum(case when UPPER(state__view__current_page__section) ='EMAIL' and message__category ='Page View' then 1 else 0 end) as webmail_page_views_count,
sum(case when state__view__current_page__section = 'My Account' and message__category ='Page View' then 1 else 0 end) as my_account_page_views_count,
sum(case when UPPER(state__view__current_page__section) = 'SUPPORT' and message__category ='Page View' then 1 else 0 end) as support_page_views_count,
sum(case when message__name IN ('View Current Bill', 'View Statement') then 1 else 0 end) as view_online_statement_count,
sum(case when message__name IN  ('Ask-Spectrum', 'Ask Spectrum') then 1 else 0 end) as ask_charter_requests_count,
sum(case when message__name IN  ('Refresh') then 1 else 0 end) as refresh_requests_count,
sum(case when state__view__current_page__section IN ('TV','On Demand') and message__category ='Page View' then 1 else 0 end) as video_page_views_count,
sum(case when message__name IN ('Play') then 1 else 0 end) as video_plays_count,
size(collect_set(case when message__name in ('my-account.create-id-final.bam',
'my-account.create-id-final.btm',
'my-account.create-id-final.bam_STVA',
'my-account.create-id-final.btm_STVA') then visit__visit_id else NULL end)) as new_ids_charter_count,

size(collect_set(case when message__name = 'my-account.create-id-final.bam'
 then visit__visit_id else NULL end)) as new_ids_charter_count_bam,
size(collect_set(case when message__name = 'my-account.create-id-final.btm'
 then visit__visit_id else NULL end)) as new_ids_charter_count_btm,
size(collect_set(case when message__name = 'my-account.create-id-final.nbtm'
 then visit__visit_id else NULL end)) as new_ids_charter_count_nbtm,
(size(collect_set (case when state__view__current_page__name in ('my-account.create-id-1.bam') then visit__visit_id else NULL end))
-
size(collect_set (case when state__view__current_page__name in ('my-account.create-id-2.bam') AND lower(message__name) RLIKE  ('.*sign-in-now.*')
then visit__visit_id else NULL end))) as attempts_create_id_count_bam,
(size(collect_set (case when state__view__current_page__name in ('my-account.create-id-1.btm') then visit__visit_id else NULL end))
-
size(collect_set (case when state__view__current_page__name in ('my-account.create-id-2.btm') AND lower(message__name) RLIKE  ('.*sign-in-now.*')
then visit__visit_id else NULL end))) as attempts_create_id_count_btm,
(size(collect_set (case when state__view__current_page__name in ('my-account.create-id-1.nbtm') then visit__visit_id else NULL end))
-
size(collect_set (case when state__view__current_page__name in ('my-account.create-id-2.nbtm') AND lower(message__name) RLIKE  ('.*sign-in-now.*')
then visit__visit_id else NULL end))) as attempts_create_id_count_nbtm,
size(collect_set(case when message__name in ('my-account.create-id-final.nbtm',
'my-account.create-id-final.nbtm_STVA') then visit__visit_id else NULL end)) as new_ids_not_charter_count,
(size(collect_set(case when state__view__current_page__name in ('my-account.create-id-1.nbtm') then visit__visit_id else NULL end))
-
size(collect_set(case when state__view__current_page__name IN ('my-account.create-id-2.nbtm')
AND message__name NOT LIKE  ('%Sign-In-Now%') then visit__visit_id else NULL end))) as total_attempts_id_off_count,
size(collect_set (case when message__name in  ('Recover-1.btm') and state__view__current_page__name != 'Recover-noID.btm' then visit__visit_id else NULL end)) as attempts_recover_username_BTM_count,
size(collect_set (case when message__name in  ('Recover-final1.btm', 'Recover-final2.btm') and state__view__current_page__name !='Recover-noID.btm' then visit__visit_id else NULL end)) as succesfull_username_recovery_BTM_count,
size(collect_set (case when message__name in  ('Recover-1.bam') and state__view__current_page__name != 'Recover-noID.bam' then visit__visit_id else NULL end)) as attempts_recover_username_BAM_count,
size(collect_set (case when message__name in  ('Recover-final1.bam', 'Recover-final2.bam') and state__view__current_page__name != 'Recover-noID.bam' then visit__visit_id else NULL end)) as succesfull_username_recovery_BAM_count,
size(collect_set (case when message__name in  ('Recover-1.nbtm') and state__view__current_page__name != 'Recover-noID.nbtm' then visit__visit_id else NULL end)) attempts_recover_username_NBTM_count ,
size(collect_set (case when message__name in  ('Recover-final1.nbtm', 'Recover-final2.nbtm') and state__view__current_page__name  != 'Recover-noID.nbtm' then visit__visit_id else NULL end)) succesfull_username_recovery_NBTM_count,

size(collect_set (case when message__name in  ('Reset-1.btm') then visit__visit_id else NULL end)) as attempts_reset_password_BTM_count,
size(collect_set (case when message__name in  ('Reset-final.btm') then visit__visit_id else NULL end)) as successful_reset_password_BTM_count,
size(collect_set (case when message__name in  ('Reset-1.bam') then visit__visit_id else NULL end)) as attempts_rest_password_BAM_count,
size(collect_set (case when message__name in  ('Reset-final.bam') then visit__visit_id else NULL end)) as successful_reset_password_BAM_count,
size(collect_set (case when message__name in  ('Reset-1.nbtm') then visit__visit_id else NULL end)) as attempts_rest_password_NBTM_count,
size(collect_set (case when message__name in  ('Reset-final.nbtm') then visit__visit_id else NULL end)) as successful_reset_password_NBTM_count,

size(collect_set (case when message__name in  ('RecoverReset-1.btm') then visit__visit_id else NULL end)) as attempts_recover_username_password_BTM_count,
size(collect_set (case when message__name in  ('RecoverReset-final1.btm', 'RecoverReset-final2.btm') then visit__visit_id else NULL end)) as successfully_recover_username_password_BTM_count,
size(collect_set (case when message__name in  ('RecoverReset-1.bam') then visit__visit_id else NULL end)) as attempts_recover_username_password_BAM_count,
size(collect_set (case when message__name in  ('RecoverReset-final1.bam', 'RecoverReset-final2.bam') then visit__visit_id else NULL end)) as successfully_recover_username_password_BAM_count,
size(collect_set (case when message__name in  ('RecoverReset-1.nbtm') then visit__visit_id else NULL end)) as attempts_recover_username_password_NBTM_count,
size(collect_set (case when message__name in  ('RecoverReset-final1.nbtm', 'RecoverReset-final2.nbtm') then visit__visit_id else NULL end)) as successfully_recover_username_password_NBTM_count,

size(collect_set (case when message__name in ('my-account.payment.one-time-debit-confirm',
'my-account.payment.one-time-credit-confirm',
'my-account.payment.one-time-eft-confirm',
'my-account.payment.one-time-debit-confirm_STVA',
'my-account.payment.one-time-credit-confirm_STVA',
'my-account.payment.one-time-eft-confirm_STVA')
then visit__visit_id else NULL end)) as one_time_payment_count ,
size(collect_set (case when  message__name IN ('my-account.payment.one-time-verify-debit',
'my-account.payment.one-time-verify-credit',
'my-account.payment.one-time-verify-eft',
'my-account.payment.one-time-verify-debit_STVA',
'my-account.payment.one-time-verify-credit_STVA',
'my-account.payment.one-time-verify-eft_STVA')
then visit__visit_id else NULL end)) as one_time_payments_confirm_count,
--
size(collect_set (case when message__name in ( 'my-account.payment.auto-pay-credit-confirm',
'my-account.payment.auto-pay-debit-confirm',
'my-account.payment.auto-pay-eft-confirm',
'my-account.payment.auto-pay-credit-confirm_STVA',
'my-account.payment.auto-pay-debit-confirm_STVA',
'my-account.payment.auto-pay-eft-confirm_STVA')
then visit__visit_id else NULL end)) as setup_autopay_count,
--
size(collect_set (case when message__name in ('my-account.payment.auto-pay-verify-credit',
'my-account.payment.auto-pay-verify-debit',
'my-account.payment.auto-pay-verify-eft',
'my-account.payment.auto-pay-verify-credit_STVA',
'my-account.payment.auto-pay-verify-debit_STVA',
'my-account.payment.auto-pay-verify-eft_STVA')
then visit__visit_id else NULL end)) as successful_autoplay_confirm_count,
size(collect_set (case when message__name = 'Save'
AND state__view__current_page__elements__name =  'Billing Notifications'
then visit__visit_id else NULL end)) as saved_bill_notifications_count,
size(collect_set (case when message__name = 'Save'
AND state__view__current_page__elements__name = 'Appointment Reminders'
then visit__visit_id else NULL end)) as saved_appoint_reminders_count,
size(collect_set (case when message__name in ('reschedule-confirm') then visit__visit_id else NULL end)) as recheduled_service_appoint_count,
size(collect_set (case when message__name in ('cancel-success-page','cancel-success-page_STVA')
then visit__visit_id else NULL end)) as cancelled_service_appoint_count,
size(collect_set (case when message__name = 'Save'
AND state__view__current_page__elements__name = 'Service Alerts'
then visit__visit_id else NULL end)) as saved_service_alerts_count,
size(collect_set (case when message__name = 'Save'
AND state__view__current_page__elements__name in ('Email','Phone')
then visit__visit_id else NULL end)) as saved_contact_information_details_count,
NULL AS attempts_create_id_count,
NULL AS attempts_rest_password_count,
NULL AS successful_reset_password_count,
date_yearmonth(partition_date) as year_month
FROM
net_events
WHERE
partition_date LIKE CONCAT(date_yearmonth(add_months("${env:TODAY_DASHED}",-1)),'%')
GROUP BY
date_yearmonth(partition_date)
;
