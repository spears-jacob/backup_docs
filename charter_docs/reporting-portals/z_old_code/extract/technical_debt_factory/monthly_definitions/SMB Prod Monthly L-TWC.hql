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
WHERE (partition_date_denver >= '2018-09-22' AND partition_date_denver < '2018-10-22')
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
-- ADOBE TWC SMB Prod Monthly Definitions
-----------------------------------------------------------------------------------------
SELECT

--Web Sessions (visits) (unique visit count by either tagging suite OR MSO split)
---2018-11 fiscal: 553,006 (553,595) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 387,058 (Re-Ran as of 2018-10-31 and number is 387,401)
---2018-09 fiscal: 426,297
---2018-08 fiscal: 441,526
SIZE(COLLECT_SET(visit__visit_id)) AS web_sessions_visits,

--Support Section Page Views (count of page views where section = 'Support')
---2018-11 fiscal:  29,611 (29,618) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal:  29,206 (Re-Ran as of 2018-10-31 and number is 29,221)
---2018-09 fiscal: 132,907
---2018-08 fiscal: 153,049
SUM(IF(((state__view__current_page__page_name NOT RLIKE '.*channelpartners.*|.*channel partners.*|.*enterprise.*')
                AND state__view__current_page__page_name RLIKE '.*support.*' AND message__category = 'Page View' )
                OR (message__triggered_by <> 'enterprise'
                  AND message__triggered_by <> 'channel partners'
                  AND state__view__current_page__page_name RLIKE '.*faq.*' AND message__category = 'Page View'),1,0)) AS support_section_page_views,

--Online Statement Views
---2018-11 fiscal: 81,596 (81,616) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 72,885 (Re-Ran as of 2018-10-31 and number is 72,915)
---2018-09 fiscal: 78,737
---2018-08 fiscal: 78,048
SUM(IF(visit__device__device_type RLIKE '.*220.*'
        AND state__view__current_page__elements__name = 'my account > billing > statements: statement download', 1, 0 )) AS view_statements,

--One-Time Payments (OTP)
---2018-11 fiscal: 72,745 (72,754) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 66,483 (Re-Ran as of 2018-10-31 and number is 66,516)
---2018-09 fiscal: 70,768
---2018-08 fiscal: 70,328
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name RLIKE '.*step 4.*'
               AND (state__view__current_page__elements__name RLIKE '.*fdp.*|.*one time.*') ,visit__visit_id, NULL))) AS one_time_payments,

--Set-up Auto-Payments (AP)
---2018-11 fiscal: 8,774 (8,774) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 8,267 (Re-Ran as of 2018-10-31 and number is 8,274)
---2018-09 fiscal: 8,945
---2018-08 fiscal: 8,827
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name RLIKE '.*step 4.*'
            AND (state__view__current_page__elements__name RLIKE '.*recurring.*') ,visit__visit_id, NULL))) AS set_up_auto_payments,

--New IDs Created (Not displaying in fiscal Sep report but need to display for fiscal Oct)
---2018-11 fiscal: 14,640 (14,643) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 13,553 (Re-Ran as of 2018-10-31 and number is 13,558)
---2018-09 fiscal: 15,561 (15,561 / 34,344 = 45.309224% Success)
---2018-08 fiscal: 15,187 (15,187 / 35,680 = 42.564462% Success)
SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*253.*', visit__visit_id, NULL))) AS ids_created,

--New ID Creation Attempts (Not displaying in fiscal Sep report but need to display for fiscal Oct)
---2018-11 fiscal: 29,854 (29,864) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 27,288 (Re-Ran as of 2018-10-31 and number is 27,297)
---2018-09 fiscal: 34,344
---2018-08 fiscal: 35,680
SIZE(COLLECT_SET(IF(visit__device__device_type RLIKE '.*249.*', visit__visit_id, NULL))) AS id_creation_attempts,

--New Sub Users Created (Not displaying in fiscal Sep report but need to display for fiscal Oct)
---2018-11 fiscal: 2,303 (2,303) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 2,170 (Re-Ran as of 2018-10-31 and number is 2,173)
---2018-09 fiscal: 2,191 (2,191 / 3,164 = 69.247788% Success)
---2018-08 fiscal: 2,106 (2,106 / 3,058 = 68.868541% Success)
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > users > add user save', visit__visit_id, NULL))) AS new_sub_users_created,

--New Sub User Creation Attempts
---2018-11 fiscal: 3,322 (3,322) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 3,130 (Re-Ran as of 2018-10-31 and number is 3,134)
---2018-09 fiscal: 3,164
---2018-08 fiscal: 3,058
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name =  'my account > users > add user', visit__visit_id, NULL))) AS new_sub_user_creation_attempts,

--Total Successful ID Recovery
---2018-11 fiscal: 2,466 (2,466) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 2,409 (SAME)
---2018-09 fiscal: 2,283 (2,283 / 15,057 = 15.16238294% Success)
---2018-08 fiscal: 2,399 (2,399 / 15,265 = 15.71568949% Success)
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username> email sent', visit__visit_id, NULL))) AS id_recovery_success,

--Total ID Recovery Attempts
---2018-11 fiscal: 14,467 (14,473) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 13,960 (Re-Ran as of 2018-10-31 and number is 13,964)
---2018-09 fiscal: 15,057
---2018-08 fiscal: 15,265
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot username > step 1', visit__visit_id, NULL))) AS id_recovery_attempts,

--Successful Password Resets (This is NOT displayed in Prod Monthly... but should be displayed if we can get the definition in Quantum)
---2018-11 fiscal: 5,359 (5,361) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 5,986 (Re-Ran as of 2018-10-31 and number is 5,987)
---2018-09 fiscal: 7,364 (7,364 / 13,948 = 52.7960998% Success)
---2018-08 fiscal: 6,071 (6,071 / 13,794 = 44.0118892% Success)
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'bc > forgot password > change password confirmation', visit__visit_id, NULL))) AS password_reset_success,

--Password Reset Attempts
---2018-11 fiscal: 13,196 (13,200) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 13,034 (Re-Ran as of 2018-10-31 and number is 13,044)
---2018-09 fiscal: 13,948
---2018-08 fiscal: 13,794
SIZE(COLLECT_SET(IF(state__view__current_page__page_name RLIKE ('bc > forgot password > step ?1'), visit__visit_id, NULL))) AS password_reset_attempts,

--Canceled Service Appointments (Used to be displayed "Cancelled" but Doug has relented and gone back to Americanized spelling)
---2018-11 fiscal: 17 (17) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 17 (SAME)
---2018-09 fiscal: 17
---2018-08 fiscal: 13
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > support > cancel appointment submit' ,visit__visit_id, NULL))) AS canceled_appointments,

--Rescheduled Service Appointments (Counting instances of appointment reschedules)
---2018-11 fiscal: 37 (37) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 36 (SAME)
---2018-09 fiscal: 40
---2018-08 fiscal: 43
SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'my account > support > reschedule appointment submit' ,visit__visit_id, NULL))) AS rescheduled_appointments,

'This concludes ADOBE TWC SMB Definitions' AS report_summary

FROM prod.asp_v_twc_bus_global_events
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')

;
