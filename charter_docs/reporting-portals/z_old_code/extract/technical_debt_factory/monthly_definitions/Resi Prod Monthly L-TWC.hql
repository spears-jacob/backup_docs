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

--Fiscal July------- CHTR: 1,973,281 | TWC: 3,680,901 | BHN: 682,307
--Fiscal August----- CHTR: 2,004,153 | TWC: 3,762,282 | BHN: 698,107
--Fiscal September-- CHTR: 2,061,430 | TWC: 3,847,515 | BHN: 733,405
--Fiscal October---- CHTR: 2,024,263 | TWC: 3,897,985 | BHN: 682,974
--Fiscal November--- CHTR: 2,017,221 | TWC: 3,984,846 | BHN: 687,168

-----------------------------------------------------------------------------------------
-- Adobe TWC Resi Prod Monthly Definitions
-----------------------------------------------------------------------------------------

SELECT
--Web Sessions (visits) (unique visit count by either tagging suite OR MSO split)
---2018-11 fiscal:  9,793,997 (9,800,320) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 10,944,664 (Re-Ran as of 2018-10-31 and number is 10,958,106)
---2018-09 fiscal: 11,307,331
SIZE(COLLECT_SET(visit__visit_id)) AS web_sessions_visits,

--Support Section Page Views (count of page views where section = 'Support')
---2018-11 fiscal: 2,196,981 (2,196,993) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 4,630,839 (Re-Ran as of 2018-10-31 and number is 4,636,196)
---2018-09 fiscal: 5,053,422
SUM(IF(LOWER(message__name) RLIKE '.*support.*'
        AND (message__category = 'Page View') , 1, 0)) AS support_section_page_views,

----View Online Statement (count of times customer clicked download link for statements)
---2018-11 fiscal:       0 (0) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 278,106
---2018-09 fiscal: 615,225
SUM(IF(state__view__current_page__elements__name RLIKE 'ebpp:statement download.*'
           AND ARRAY_CONTAINS(message__feature__name,'Instance of eVar7'),1,0)) AS view_statements,

----One-Time Payments (OTP) (This is visits that did OTP)
---2018-11 fiscal: 2,464,779 (2,466,439) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 2,428,028 (Re-Ran as of 2018-10-31 and number is 2,431,050)
---2018-09 fiscal: 2,506,960
SIZE(COLLECT_SET(IF(operation__operation_type RLIKE '.*one time.*'
                AND state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you'
                AND message__category = 'Page View', visit__visit_id, NULL))) AS one_time_payments,

----Set-up Auto-Payments (AP) (This is visits that did AP Setup)
---2018-11 fiscal: 158,485 (158,587) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 172,221 (Re-Ran as of 2018-10-31 and number is 172,480)
---2018-09 fiscal: 191,761
SIZE(COLLECT_SET(IF(operation__operation_type RLIKE '.*recurring:.*'
                AND state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you'
                AND message__category = 'Page View', visit__visit_id, NULL))) AS set_up_auto_payments,

--Total New IDs Created (This number (numerator) divided by total attempts gets our % success)
---2018-11 fiscal: 263,751 (263,948) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 278,324 (Re-Ran as of 2018-10-31 and number is 278,670)
---2018-09 fiscal: 323,978 (323,978 / 877,978 = 36.900469% Success)
SUM(IF(array_contains(message__feature__name,'Custom Event 5'),1,0)) AS ids_created,

--Total New ID Create Attempts (This number (denominator) divided by total attempts gets our % success)
---2018-11 fiscal: 721,827 (722,392) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 782,141 (Re-Ran as of 2018-10-31 and number is 783,126)
---2018-09 fiscal: 877,978
SUM(IF(array_contains(message__feature__name,'Custom Event 1'),1,0)) AS id_creation_attempts,

--Successful Password Resets (This number (numerator) divided by total attempts gets our % success)
---2018-11 fiscal: 438,618 (438,962) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 501,810 (Re-Ran as of 2018-10-31 and number is 502,464)
---2018-09 fiscal: 549,198 (549,198 / 855,195 = 64.2190378% Success)
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > reset password > change password confirmation',visit__visit_id,NULL))) AS password_reset_success,

--Password Reset Attempts (This number (denominator) divided by total attempts gets our % success)
---2018-11 fiscal: 760,653 (761,242) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 846,515 (Re-Ran as of 2018-10-31 and number is 847,632)
---2018-09 fiscal: 855,195
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > reset password > step 1',visit__visit_id, NULL))) AS password_reset_attempts,

--Total Successful ID Recovery (This number (numerator) divided by total attempts gets our % success)
---2018-11 fiscal: 381,364 (381,671) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 432,983 (Re-Ran as of 2018-10-31 and number is 433,558)
---2018-09 fiscal: 438,800 (438,800 / 687,324 = 63.841798046% Success)
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > retrieve username > username displayed'
                AND message__category = 'Page View', visit__visit_id, NULL))) AS id_recovery_success,

--Total ID Recovery Attempts (This number (denominator) divided by total attempts gets our % success)
---2018-11 fiscal: 606,539 (607,005) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 675,403 (Re-Ran as of 2018-10-31 and number is 676,300)
---2018-09 fiscal: 687,324
SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > retrieve username > lookup account', visit__visit_id, NULL))) AS id_recovery_attempts,

--Refresh Digital Receivers Requests (Counting instances of tv equipment reset starts)
---2018-11 fiscal: 36,624 (36,652) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 42,862 (Re-Ran as of 2018-10-31 and number is 42,927)
---2018-09 fiscal: 42,409
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section RLIKE "^ats:troubleshoot:tv.*confirmation$", visit__visit_id, NULL))) AS refresh_digital_receiver_requests,

----Modem Router Resets (Counting instances of internet equipment reset starts)
---2018-11 fiscal:  96,034 (96,133) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 100,887 (Re-Ran as of 2018-10-31 and number is 101,096)
---2018-09 fiscal: 100,782
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section RLIKE "^ats:troubleshoot:homephone.*confirmation$"
                 OR state__view__current_page__sub_section RLIKE "^ats:troubleshoot:internet.*confirmation$", visit__visit_id, NULL))) AS modem_router_resets,

----Rescheduled Service Appointments (Counting instances of appointment reschedules)
---2018-11 fiscal: 4,530 (4,535) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 4,698 (Re-Ran as of 2018-10-31 and number is 4,699)
---2018-09 fiscal: 5,524
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section = 'services : my services : appointment manager : reschedule submitted', visit__visit_id, NULL))) AS rescheduled_appointments,

----Canceled Service Appointments (Used to be displayed "Cancelled" but Doug has relented and gone back to Americanized spelling)
---2018-11 fiscal: 5,825 (5,829) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 6,283 (Re-Ran as of 2018-10-31 and number is 6,289)
---2018-09 fiscal: 6,688
SIZE(COLLECT_SET(IF(state__view__current_page__sub_section = 'my services > appointment manager > cancel submitted', visit__visit_id, NULL))) AS canceled_appointments,

'This concludes Adobe TWC Resi Definitions' AS report_summary

FROM prod.asp_v_twc_residential_global_events
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')

;
