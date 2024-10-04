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

--Fiscal July------- CHTR: 1,973,281 | TWC: 3,680,901 | BHN: 682,307
--Fiscal August----- CHTR: 2,004,153 | TWC: 3,762,282 | BHN: 698,107
--Fiscal September-- CHTR: 2,061,430 | TWC: 3,847,515 | BHN: 733,405
--Fiscal October---- CHTR: 2,024,263 | TWC: 3,897,985 | BHN: 682,974
--Fiscal November--- CHTR: 2,017,221 | TWC: 3,984,846 | BHN: 687,168

-----------------------------------------------------------------------------------------
-- Adobe BHN Resi Prod Monthly Definitions pt1 (visits, support section page views)
-----------------------------------------------------------------------------------------
SELECT
--Web Sessions (visits) (unique visit count by either tagging suite OR MSO split)
---2018-11 fiscal: 1,351,495 (1,352,064) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 1,633,193 (Re-Ran as of 2018-10-31 and number is 1,634,196)
---2018-09 fiscal: 1,964,794
---2017-10 fiscal: 4,113,778
SIZE(COLLECT_SET(IF(visit__visit_id IS NOT NULL, visit__visit_id, NULL))) AS web_sessions_visits,

--Support Section Page Views (count of page views where section = 'support active')
---2018-11 fiscal: 120,429 (120,488) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 459,472 (Re-Ran as of 2018-10-31 and number is 459,533)
---2018-09 fiscal: 738,001
---2017-10 fiscal: 875,682 (2017-10 Report shows "2.1M")
SUM(IF(LOWER(visit__settings["post_prop6"]) = 'support active'
         AND message__category = 'Page View', 1,0)) AS support_section_page_views,

'This concludes Adobe BHN Resi Definitions pt1' AS report_summary

FROM prod.asp_v_bhn_residential_events
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')

;

-------------------------------------------------------------------------------------------------------------------------------------------
-- Adobe BHN Resi Prod Monthly Definitions from Billing Suite (view online statement, One-Time Payments (OTP), set-up auto-payments (ap))
-------------------------------------------------------------------------------------------------------------------------------------------
SELECT
----View Online Statement (count of times customer clicked download link for statements)
---2018-11 fiscal: 282,698 (282,811) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 276,039 (Re-Ran as of 2018-10-31 and number is 276,235)
---2018-09 fiscal: 292,218
---2017-10 fiscal: 323,632 (2017-10 Report showed "323,728" likely the omniture nbr -0.03% diff)
SUM(IF(message__name RLIKE('.*stmtdownload/.*')
   AND message__category = 'Exit Link'
   AND visit__settings["post_evar9"] = 'RES', 1, 0)) AS view_statements,

----One-Time Payments (OTP) (This is visits that did OTP)
---2018-11 fiscal: 429,619 (429,790) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 400,100 (Re-Ran as of 2018-10-31 and number is 400,372)
---2018-09 fiscal: 435,663
---2017-10 fiscal: 400,651 (2017-10 Report showed "380,023" Not sure which we'll use)
sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 31')
                  AND state__view__current_page__page_type='RES',1,0)) AS one_time_payments,

----Set-up Auto-Payments (AP) (This is visits that did AP Setup)
---2018-11 fiscal: 22,147 (22,153) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 21,556 (Re-Ran as of 2018-10-31 and number is 21,569)
---2018-09 fiscal: 23,872
---2017-10 fiscal: 15,556 (2017-10 Report showed "14,374" 8.22% diff)
sum(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 24')
                  AND state__view__current_page__page_type='RES',1,0)) AS set_up_auto_payments,

'This concludes Adobe BHN Resi Billing Definitions' AS report_summary

FROM prod.asp_v_bhn_bill_pay_events
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')

;

-----------------------------------------------------------------------------------------
-- Adobe BHN Resi Prod Monthly Definitions pt2 (Create, Reset, Recover, Equipment, Appointments)
-----------------------------------------------------------------------------------------
SELECT

--Total New IDs Created (This number (numerator) divided by total attempts gets our % success)
---2018-11 fiscal: 30,134 (30,146) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 30,966 (Re-Ran as of 2018-10-31 and number is 30,991)
---2018-09 fiscal: 35,306 (35,306 / 72,925 = 48.4141241% Success)
---2017-10 fiscal:
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'registration success'
                      AND message__category = 'Custom Link', visit__visit_id, NULL))) AS ids_created,

--Total New ID Create Attempts (This number (denominator) divided by total attempts gets our % success)
---2018-11 fiscal: 59,032 (59,058) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 62,362 (Re-Ran as of 2018-10-31 and number is 62,411)
---2018-09 fiscal: 72,925
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'registration submit'
                      AND message__category = 'Custom Link', visit__visit_id, NULL))) AS id_creation_attempts,

--Successful Password Resets (**NOTE** We've relied on BPasciak EMail for this.
                                    --The below definition was supplied via ALove/GKnasel.
                                    --This number (numerator) divided by total attempts gets our % success)
---2018-11 fiscal: 50,155 (50,188) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 60,936 (Re-Ran as of 2018-10-31 and number is 60,964)
---2018-09 fiscal: 70,332 (70,332 / 127,024 = 55.3690641% Success)
---2018-09 fiscal BPasciak eMail Nbrs: 105,666 (105,666 / 240,125 = 44.0045809% Success)
---2018-08 fiscal: 63,664 (63,664 / 106,801 = 59.609929% Success)
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'reset password success'
                      AND message__category = 'Custom Link', visit__visit_id, NULL))) AS password_reset_success,

--Password Reset Attempts (**NOTE** We've relied on BPasciak EMail for this.
                                    --The below definition was supplied via ALove/GKnasel.
                                    --This number (denominator) divided by total attempts gets our % success)
---2018-11 fiscal: 110,877 (110,948) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 109,016 (Re-Ran as of 2018-10-31 and number is 109,101)
---2018-09 fiscal: 127,024
---2018-09 fiscal BPasciak eMail Nbrs: 240,125
---2018-08 fiscal: 106,801
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'reset password step1'
                      AND message__category = 'Custom Link', visit__visit_id, NULL))) AS password_reset_attempts,

--Total Successful ID Recovery (This number (numerator) divided by total attempts gets our % success)
---2018-11 fiscal: 41,308 (41,335) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 40,087 (Re-Ran as of 2018-10-31 and number is 40,111)
---2018-09 fiscal: 46,832 (46,832 / 84,649 = 55.32493% Success)
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'recover username success'
                      AND message__category = 'Custom Link', visit__visit_id, NULL))) AS id_recovery_success,

--Total ID Recovery Attempts (This number (denominator) divided by total attempts gets our % success)
---2018-11 fiscal: 73,309 (73,352) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 71,124 (Re-Ran as of 2018-10-31 and number is 71,175)
---2018-09 fiscal: 84,649
SIZE(COLLECT_SET(IF( LOWER(message__name) = 'recover username step1'
                      AND message__category = 'Custom Link', visit__visit_id, NULL))) AS id_recovery_attempts,

--Refresh Digital Receivers Requests (NO DEFINITION)
---2018-11 fiscal: NULL
---2018-10 fiscal: NULL
---2018-09 fiscal: NULL
NULL AS refresh_digital_receivers_requests,

--Modem Router Resets (NO DEFINITION)
---2018-11 fiscal: NULL
---2018-10 fiscal: NULL
---2018-09 fiscal: NULL
NULL AS modem_router_resets,

--Rescheduled Service Appointments (NO DEFINITION)
---2018-11 fiscal: NULL
---2018-10 fiscal: NULL
---2018-09 fiscal: NULL
NULL AS rescheduled_appointments,

--Canceled Service Appointments (NO DEFINITION)
---2018-11 fiscal: NULL
---2018-10 fiscal: NULL
---2018-09 fiscal: NULL
NULL AS canceled_service_appointments,

'This concludes Adobe BHN Resi Definitions pt2' AS report_summary

FROM prod.asp_v_bhn_residential_events
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')

;
