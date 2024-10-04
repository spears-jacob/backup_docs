set hive.exec.max.dynamic.partitions=20000
;
SET hive.exec.max.dynamic.partitions.pernode=7000
;


USE ${env:ENVIRONMENT};

-- Drop and rebuild L-TWC Business Global  --
DROP TABLE IF EXISTS ${env:TMP_db}.sb_twc_${env:CADENCE};

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.sb_twc_${env:CADENCE} AS

SELECT  --Page Views
        SUM(IF(message__triggered_by IN('cla','my account') AND message__category = 'Page View',1, 0)) AS my_account_page_views, --updated 2017-12-26 DPrince
        --Self-Help Page
        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > support > reschedule appointment submit' ,visit__visit_id, NULL))) AS rescheduled_service_appointments, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > support > cancel appointment submit' ,visit__visit_id, NULL))) AS cancelled_service_appointments, -- updated 2017-12-26 DPrince
        --Support
        SUM(IF(((state__view__current_page__page_name NOT RLIKE '.*channelpartners.*|.*channel partners.*|.*enterprise.*')
                AND state__view__current_page__page_name RLIKE '.*support.*' AND message__category = 'Page View' )
                OR (message__triggered_by <> 'enterprise'
                AND message__triggered_by <> 'channel partners'
                AND state__view__current_page__page_name RLIKE '.*faq.*' AND message__category = 'Page View'),1,0)) AS support_page_views, --updated 2017-12-26 DPrince
        --OBP
        SUM(IF(visit__device__device_type RLIKE '.*220.*' AND state__view__current_page__elements__name = 'my account > billing > statements: statement download', 1, 0 )) AS online_statement_views, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name RLIKE '.*step 4.*'
            AND (state__view__current_page__elements__name RLIKE '.*fdp.*|.*one time.*') ,visit__visit_id, NULL))) AS one_time_payments, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name RLIKE '.*step 4.*'
            AND (state__view__current_page__elements__name RLIKE '.*recurring.*') ,visit__visit_id, NULL))) AS auto_pay_setup_successes, --updated 2017-12-26 DPrince
        --New Id Creation
        ---total
        SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*249.*', visit__visit_id, NULL))) AS total_account_creation_attempts, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*253.*', visit__visit_id, NULL))) AS total_new_accounts_created, --updated 2017-12-26 Dprince
        ---on net
        SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*249.*' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS new_account_creation_attempts_on_chtr_network, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*253.*' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS new_accounts_created_on_chtr_network, --updated 2017-12-26 DPrince
        ---off net
        SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*249.*' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS new_account_creation_attempts_off_chtr_network, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*253.*' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS new_accounts_created_off_chtr_network, --updated 2017-12-26 DPrince
        --Sub-User Creation
        ---total
        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name =  'my account > users > add user', visit__visit_id, NULL))) AS total_sub_user_creation_attempts, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > users > add user save', visit__visit_id, NULL))) AS total_new_sub_users_created, --updated 2017-12-26 DPrince
        ---on net
        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name =  'my account > users > add user' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS sub_user_creation_attempts_on_chtr_network, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > users > add user save' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS sub_users_created_on_chtr_network, --updated 2017-12-26 DPrince
        ---off net
        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name =  'my account > users > add user' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS sub_user_creation_attempts_off_chtr_network, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > users > add user save' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS sub_users_created_off_chtr_network, --updated 2017-12-26 DPrince
        --Username Recovery
        ---total
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username > step 1', visit__visit_id, NULL))) AS total_username_recovery_attempts, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username> email sent', visit__visit_id, NULL))) AS total_username_recovery_successes, --updated 2017-12-26 DPrince
        ---on net
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username > step 1' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS username_recovery_attempts_on_chtr_network, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username> email sent' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS username_recovery_successes_on_chtr_network, --updated 2017-12-26 DPrince
        ---off net
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username > step 1' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS username_recovery_attempts_off_chtr_network, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username> email sent' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS username_recovery_successes_off_chtr_network, --updated 2017-12-26 DPrince
        --Password Reset
        ---total
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name RLIKE ('bc > forgot password > step ?1'), visit__visit_id, NULL))) AS total_attempts_to_reset_password, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot password > change password confirmation', visit__visit_id, NULL))) AS total_successful_password_resets, --updated 2017-12-26 DPrince
        --- on net
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name RLIKE ('bc > forgot password > step ?1') AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS attempts_to_reset_password_on_chtr_network, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot password > change password confirmation' AND visit__isp__isp IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS successful_password_resets_on_chtr_network, --updated 2017-12-26 DPrince
        --- off net
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name RLIKE ('bc > forgot password > step ?1') AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS attempts_to_reset_password_off_chtr_network, --updated 2017-12-26 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot password > change password confirmation' AND visit__isp__isp NOT IN('rr.com','twcable.com','twcbiz.com'), visit__visit_id, NULL))) AS successful_password_resets_off_chtr_network, --updated 2017-12-26 DPrince
        ---Prod Monthly Payment Fields
        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name RLIKE '.*step 4.*'
            AND (state__view__current_page__elements__name RLIKE '.*fdp.*|.*one time.*') ,visit__visit_id, NULL))) AS one_time_payment_updated_wotap,

        SIZE(COLLECT_SET(IF(state__view__current_page__elements__name RLIKE '.*step 4.*'
            AND (state__view__current_page__elements__name RLIKE '.*recurring.*') ,visit__visit_id, NULL))) AS auto_payment_confirm_updated_wotap,
        SIZE(COLLECT_SET(IF(visit__visit_id IS NOT NULL, visit__visit_id, NULL))) AS visits,
        'L-TWC' AS company,
        ${env:pf}
 FROM  (SELECT ${env:ap} as ${env:pf},
               message__category,
               message__triggered_by,
               state__view__current_page__elements__name,
               state__view__current_page__page_name,
               visit__device__device_type,
               visit__isp__isp,
               visit__visit_id
        FROM asp_v_twc_bus_global_events
        LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
        WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
      ) dictionary
 GROUP BY ${env:pf}
 ;


 INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

 SELECT value, unit, domain, company, ${env:pf}, metric
 FROM  ( SELECT  'my_account_page_views' as metric,
                my_account_page_views AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'rescheduled_service_appointments' as metric,
                rescheduled_service_appointments AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'cancelled_service_appointments' as metric,
                cancelled_service_appointments AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'support_page_views' as metric,
                support_page_views AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'online_statement_views' as metric,
                online_statement_views AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'one_time_payments' as metric,
                one_time_payments AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'auto_pay_setup_successes' as metric,
                auto_pay_setup_successes AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'total_account_creation_attempts' as metric,
                total_account_creation_attempts AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'total_new_accounts_created' as metric,
                total_new_accounts_created AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'new_account_creation_attempts_on_chtr_network' as metric,
                new_account_creation_attempts_on_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'new_accounts_created_on_chtr_network' as metric,
                new_accounts_created_on_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'new_account_creation_attempts_off_chtr_network' as metric,
                new_account_creation_attempts_off_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'new_accounts_created_off_chtr_network' as metric,
                new_accounts_created_off_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'total_sub_user_creation_attempts' as metric,
                total_sub_user_creation_attempts AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'total_new_sub_users_created' as metric,
                total_new_sub_users_created AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'sub_user_creation_attempts_on_chtr_network' as metric,
                sub_user_creation_attempts_on_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'sub_users_created_on_chtr_network' as metric,
                sub_users_created_on_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'sub_user_creation_attempts_off_chtr_network' as metric,
                sub_user_creation_attempts_off_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'sub_users_created_off_chtr_network' as metric,
                sub_users_created_off_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'total_username_recovery_attempts' as metric,
                total_username_recovery_attempts AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'total_username_recovery_successes' as metric,
                total_username_recovery_successes AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'username_recovery_attempts_on_chtr_network' as metric,
                username_recovery_attempts_on_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'username_recovery_successes_on_chtr_network' as metric,
                username_recovery_successes_on_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'username_recovery_attempts_off_chtr_network' as metric,
                username_recovery_attempts_off_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'username_recovery_successes_off_chtr_network' as metric,
                username_recovery_successes_off_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'total_attempts_to_reset_password' as metric,
                total_attempts_to_reset_password AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'total_successful_password_resets' as metric,
                total_successful_password_resets AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'attempts_to_reset_password_on_chtr_network' as metric,
                attempts_to_reset_password_on_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'successful_password_resets_on_chtr_network' as metric,
                successful_password_resets_on_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'attempts_to_reset_password_off_chtr_network' as metric,
                attempts_to_reset_password_off_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'successful_password_resets_off_chtr_network' as metric,
                successful_password_resets_off_chtr_network AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'one_time_payment_updated_wotap' as metric,
                one_time_payment_updated_wotap AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'auto_payment_confirm_updated_wotap' as metric,
                auto_payment_confirm_updated_wotap AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
         UNION
         SELECT  'visits' as metric,
                visits AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
         FROM ${env:TMP_db}.sb_twc_${env:CADENCE}
     ) q
 ;
