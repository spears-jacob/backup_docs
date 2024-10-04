--------------------------------------------------------------------
-- Unique HHs Logged In (provides total sub count that logged in)
---CHTR, TWC, BHN
--------------------------------------------------------------------

SELECT
  source_app
--  , COUNT(trace_id) as total_login_attempts -- trace_ids (aka transaction_id) are unique per attempt
--  , COUNT(if(is_success = true,trace_id,null)) as total_successes  --boolean flag if the attempt was successful or not
--  , SIZE(COLLECT_SET(IF(username_aes256 IS NOT NULL,username_aes256, NULL))) as users_logged_in -- mimicking what was done on the old version
  , SIZE(COLLECT_SET(IF(account_number_aes256 IS NOT NULL,account_number_aes256, NULL))) as hhs_logged_in -- mimicking what was done on the old version
  , CASE WHEN is_success = true then footprint ELSE 'UNAUTH' END as company

FROM asp_v_federated_identity
WHERE (partition_date_denver >= '2018-10-22' AND partition_date_denver < '2018-11-22')
      AND LOWER(source_app) IN('portals-idp','myspectrum','portals-idp-comm')
      AND account_number_aes256 IS NOT NULL
      AND is_success = true
GROUP BY source_app, case when is_success = true then footprint else 'UNAUTH' END
;

--Fiscal July------- CHTR: 81,755 | TWC: 115,363 | BHN: 25,363  << -- Different from report due to date treatment from auth team.
-- ^^These are correct and report numbers are not^^
--Fiscal August----- CHTR: 85,683 | TWC: 125,916 | BHN: 27,046
--Fiscal September-- CHTR: 86,928 | TWC: 129,251 | BHN: 28,462
--Fiscal October---- CHTR: 84,343 | TWC: 122,660 | BHN: 25,600
--Fiscal November--- CHTR: 90,561 | TWC: 135,268 | BHN: 27,303

-----------------------------------------------------------------------------------------
-- ADOBE BHN SMB Prod Monthly Definitions pt.1 Overall
-----------------------------------------------------------------------------------------
SELECT

--Web Sessions (visits) (unique visit count by either tagging suite OR MSO split)
---2018-11 fiscal: 58,098 (58,105) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 48,382
---2018-09 fiscal: 67,416
---2018-08 fiscal: 66,618
 SIZE(COLLECT_SET(visit__visit_id)) AS web_sessions_visits,

--Support Section Page Views (count of page views where section = 'Support')
---2018-11 fiscal: 140 (140) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 1
---2018-09 fiscal: 9,052 (Report has: 9,054... diff of 2)
---2018-08 fiscal: 12,324 (Report has: 12,329... diff of 5)
SUM(IF(message__category = 'Page View'
   AND visit__settings['post_prop9'] RLIKE '.*small-medium.*'
   AND concat_ws(',',(message__name)) RLIKE ".*medium:support.*", 1, 0)) AS support_section_page_views,

'This concludes ADOBE BHN SMB Definitions pt.1' AS report_summary

FROM prod.asp_v_bhn_my_services_events
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')

;

-----------------------------------------------------------------------------------------
-- ADOBE BHN SMB Prod Monthly Definitions pt.2 Billing
-----------------------------------------------------------------------------------------
SELECT

--Online Statement Views
---2018-11 fiscal: 20,772 (20,774) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 19,850 (19,859)
---2018-09 fiscal: 21,679
---2018-08 fiscal: 20,888
SUM(IF(message__name RLIKE('.*stmtdownload/.*')
   AND message__category = 'Exit Link'
   AND visit__settings["post_evar9"] = 'SMB', 1, 0)) AS view_statements,

--One-Time Payments (OTP)
---2018-11 fiscal: 17,687 (17,688) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 14,648 (14,659)
---2018-09 fiscal: 16,934
---2018-08 fiscal: 17,160
SUM(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 31')
                  AND state__view__current_page__page_type='SMB',1,0)) AS one_time_payments,

--Set-up Auto-Payments (AP)
---2018-11 fiscal: 1,797 (1,797) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 1,414 (SAME)
---2018-09 fiscal: 1,617
---2018-08 fiscal: 1,512
SUM(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 24')
                  AND state__view__current_page__page_type='SMB',1,0)) AS set_up_auto_payments,

'This concludes ADOBE BHN SMB Definitions pt.2' AS report_summary

FROM prod.asp_v_bhn_bill_pay_events
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')

;

-----------------------------------------------------------------------------------------
-- ADOBE BHN SMB Prod Monthly Definitions pt.3 Create, Reset, Recover
-----------------------------------------------------------------------------------------
SELECT

--New IDs Created (Not displaying in fiscal Sep report but need to display for fiscal Oct)
---2018-11 fiscal: 1,703 (1704) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 1,468 (SAME)
---2018-09 fiscal: 2,151
---2018-08 fiscal: 2,195
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
 AND ARRAY_CONTAINS(message__name, 'Registration Success'), visit__visit_id, NULL))) AS ids_created,

--New ID Creation Attempts (Not displaying in fiscal Sep report but need to display for fiscal Oct)
---2018-11 fiscal: 3,736 (3,737) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 3,064 (SAME)
---2018-09 fiscal: 4,783
---2018-08 fiscal: 4,413
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
 AND ARRAY_CONTAINS(message__name, 'Registration Submit') ,visit__visit_id, NULL))) AS id_creation_attempts,

--New Sub Users Created (Not displaying in fiscal Sep report but need to display for fiscal Oct)
---2018-11 fiscal:
---2018-10 fiscal:
---2018-09 fiscal:
---2018-08 fiscal:
-- AS new_sub_users_created,

--New Sub User Creation Attempts
---2018-11 fiscal:
---2018-10 fiscal:
---2018-09 fiscal:
---2018-08 fiscal:
-- AS new_sub_user_creation_attempts,

--Total Successful ID Recovery
---2018-11 fiscal:   924 (924)Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal:   615 (SAME)
---2018-09 fiscal: 1,141
---2018-08 fiscal:   961
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
 AND ARRAY_CONTAINS(message__name, 'Recover Username Success') ,visit__visit_id, NULL))) AS id_recovery_success,

--Total ID Recovery Attempts
---2018-11 fiscal: 1,829 (1,829) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 1,190 (1,192)
---2018-09 fiscal: 2,137
---2018-08 fiscal: 1,762
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
 AND ARRAY_CONTAINS(message__name, 'Recover Username Step1') ,visit__visit_id, NULL))) AS id_recovery_attempts,

--Successful Password Resets (This is NOT displayed in Prod Monthly... but should be displayed if we can get the definition in Quantum)
---2018-11 fiscal:   897 (898) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal:   736 (737)
---2018-09 fiscal: 1,173
---2018-08 fiscal: 1,097
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
 AND ARRAY_CONTAINS(message__name, 'Reset Password Success') ,visit__visit_id, NULL))) AS password_reset_success,

--Password Reset Attempts
---2018-11 fiscal: 2,499 (2,500) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 1,701 (1,702)
---2018-09 fiscal: 2,705
---2018-08 fiscal: 2,263
SIZE(COLLECT_SET(IF(message__category = 'Custom Link'
 AND ARRAY_CONTAINS(message__name, 'Reset Password Step1') ,visit__visit_id, NULL))) AS password_reset_attempts,

--Canceled Service Appointments (Used to be displayed "Cancelled" but Doug has relented and gone back to Americanized spelling)
---2018-11 fiscal:
---2018-10 fiscal:
---2018-09 fiscal:
---2018-08 fiscal:
 --AS canceled_service_appointments,

--Rescheduled Service Appointments (Counting instances of appointment reschedules)
---2018-11 fiscal:
---2018-10 fiscal:
---2018-09 fiscal:
---2018-08 fiscal:
 --AS rescheduled_service_appointments,

'This concludes ADOBE BHN SMB Definitions pt.3' AS report_summary

FROM prod.asp_v_bhn_my_services_events
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')
;
