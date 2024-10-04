-------------------------------------------------------------------------------

--Populates the temp table net_products_agg_monthly with aggregated data for L-TWC

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};


-- Drop and rebuild net_bill_pay_analytics_monthly temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.net_twc_${env:CADENCE};

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.net_twc_${env:CADENCE}
AS

SELECT  SUM(IF(message__category = 'Page View',1,0)) AS total_page_views_count,
        SUM(IF(message__category = 'Page View' and state__view__current_page__section = 'services',1,0)) AS my_account_page_views_count,
        SUM(IF((LOWER(message__name) RLIKE '.*support.*') AND (message__category = 'Page View') , 1, 0)) AS support_page_views_count, --updated 2018-04-26 DPrince
---- removed from code 2018-01-02
--      SIZE(COLLECT_SET(IF(LOWER(state__search__text) Rlike ".*askamy.*",visit__visit_id,NULL))) AS ask_charter_requests_count,
        SIZE(COLLECT_SET(IF(state__view__current_page__sub_section RLIKE "^ats:troubleshoot:tv.*confirmation$",visit__visit_id,NULL))) AS refresh_requests_count,
        SIZE(COLLECT_SET(IF(state__view__current_page__sub_section = 'services : my services : appointment manager : reschedule submitted',visit__visit_id,NULL))) AS rescheduled_service_appoint_count, --updated 2017-11-24 DPrince
        SIZE(COLLECT_SET(IF(state__view__current_page__sub_section = 'my services > appointment manager > cancel submitted',visit__visit_id,NULL))) AS cancelled_service_appoint_count,
----  removed from code 2017-12-28
--      SUM(IF(array_contains(message__feature__name,'Custom Event 5') and visit__connection__network_status='cla 3.0:in home',1,0)) AS new_ids_charter_count_on_net,
--      SUM(IF(array_contains(message__feature__name,'Custom Event 5') and visit__connection__network_status='cla 3.0:out of home',1,0)) AS new_ids_charter_count_off_net,
-- Added new ids including sub accounts on 2018-03-04
        SUM(IF(array_contains(message__feature__name,'Custom Event 5'),1,0))
          +
        SUM(IF(state__view__current_page__elements__name = 'mys:user management:user added' AND visit__device__device_type rlike '.*106.*',1,0)) AS new_ids_incl_sub_accts,
        SUM(IF(array_contains(message__feature__name,'Custom Event 1'),1,0)) AS attempts_create_id_count_all,
        SUM(IF(array_contains(message__feature__name,'Custom Event 1') and visit__connection__network_status='cla 3.0:in home',1,0)) AS attempts_create_id_count_on_net,
        SUM(IF(array_contains(message__feature__name,'Custom Event 1') and visit__connection__network_status='cla 3.0:out of home',1,0)) AS attempts_create_id_count_off_net,
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > retrieve username > lookup account',visit__visit_id,NULL))) AS attempts_recover_username_count_all,
        SIZE(COLLECT_SET(if(state__view__current_page__page_name = 'cla > retrieve username > username displayed' AND message__category = 'Page View', visit__visit_id,NULL))) AS successful_username_recovery_count_all,
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > reset password > step 1',visit__visit_id,NULL))) AS attempts_reset_password_count_all,
        SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > reset password > change password confirmation',visit__visit_id,NULL))) AS successful_reset_password_count_all,
        SIZE(COLLECT_SET(IF(state__view__current_page__sub_section RLIKE "^ats:troubleshoot:homephone.*confirmation$" OR state__view__current_page__sub_section RLIKE "^ats:troubleshoot:internet.*confirmation$",visit__visit_id,NULL))) AS modem_router_resets,
        SIZE(COLLECT_SET(IF(visit__visit_id IS NOT NULL, visit__visit_id, NULL))) AS visits,
        'L-TWC' AS company,
        ${env:pf}
FROM  (SELECT ${env:ap} as ${env:pf},
              message__category,
              message__name,
              message__feature__name,
              state__search__text,
              state__view__current_page__page_name,
              state__view__current_page__section,
              state__view__current_page__sub_section,
              visit__connection__network_status,
              visit__visit_id,
              operation__operation_type,
              state__view__current_page__elements__name,
              visit__device__device_type
       FROM asp_v_twc_residential_global_events
       LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
       WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
     ) dictionary
GROUP BY ${env:pf}
;



SELECT '*****-- End L-TWC net_products_agg_monthly Temp Table Insert --*****' -- 480.997 seconds
;

-- End L-TWC net_products_agg_monthly Temp Table Insert --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  ( SELECT  'total_page_views_count' as metric,
                 total_page_views_count AS value,
                 'page views' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'my_account_page_views_count' as metric,
                 my_account_page_views_count AS value,
                 'instances' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'support_page_views_count' as metric,
                 support_page_views_count AS value,
                 'page_views' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
---- removed from code 2018-01-02
--      SELECT  'ask_charter_requests_count' as metric,
--               ask_charter_requests_count AS value,
--              'page views' as unit,
--              'resi' as domain,
--               company,
--               ${env:pf}
--        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
--        UNION
        SELECT  'refresh_requests_count' as metric,
                 refresh_requests_count AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'rescheduled_service_appoint_count' as metric,
                 rescheduled_service_appoint_count AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'cancelled_service_appoint_count' as metric,
                 cancelled_service_appoint_count AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
-- removed from code 2017-12-28
--        SELECT  'new_ids_charter_count_on_net' as metric,
--                 new_ids_charter_count_on_net AS value,
--                 'visits' as unit,
--                 'resi' as domain,
--                 company,
--                 ${env:pf}
--        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
--        UNION
--        SELECT  'new_ids_charter_count_off_net' as metric,
--                 new_ids_charter_count_off_net AS value,
--                 'visits' as unit,
--                 'resi' as domain,
--                 company,
--                 ${env:pf}
--        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
--        UNION
---- Added new ids including sub accounts on 2018-03-04
        SELECT 'new_ids_incl_sub_accts' as metric,
               new_ids_incl_sub_accts AS value,
               'visits' as unit,
               'resi' as domain,
               company,
               ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'attempts_create_id_count_all' as metric,
                 attempts_create_id_count_all AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'attempts_create_id_count_on_net' as metric,
                 attempts_create_id_count_on_net AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'attempts_create_id_count_off_net' as metric,
                 attempts_create_id_count_off_net AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'attempts_recover_username_count_all' as metric,
                 attempts_recover_username_count_all AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'successful_username_recovery_count_all' as metric,
                 successful_username_recovery_count_all AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'attempts_reset_password_count_all' as metric,
                 attempts_reset_password_count_all AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'successful_reset_password_count_all' as metric,
                 successful_reset_password_count_all AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'modem_router_resets' as metric,
                 modem_router_resets AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
        UNION
        SELECT  'visits' as metric,
                 visits AS value,
                 'visits' as unit,
                 'resi' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.net_twc_${env:CADENCE}
      ) q
;
