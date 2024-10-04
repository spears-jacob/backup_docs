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
-- Quantum CHTR Resi Prod Monthly Definitions
-----------------------------------------------------------------------------------------

SELECT
--Web Sessions (visits) (unique visit count by either tagging suite OR MSO split)
---2018-11 fiscal: 25,939,658 (Ran this on 2018-11-22 and again on 2018-11-23 and numbers matched)
---2018-10 fiscal: 25,346,018
---2018-09 fiscal: 25,983,984
SIZE(COLLECT_SET(visit__visit_id)) AS web_sessions_visits,

--Support Section Page Views (count of page views where section = 'Support')
---**NOTE** consolidated search and support has the respective TWC and BHN
---...numbers declining to practically 0 page views for 2018-10
---Going forward we'll work to convey the total as 'SpecNet'
---...for the Company with NO Legacy Company split out
---2018-11 fiscal: 11,800,429
---2018-10 fiscal:  8,958,467
---2018-09 fiscal:  8,295,619
SUM(IF(visit__application_details__application_name = 'SpecNet'
  AND message__name = 'pageView'
  AND state__view__current_page__app_section = 'support', 1, 0)) AS support_section_page_views,

--View Online Statement (count of times customer clicked download link for statements)
---2018-11 fiscal: 221,814
---2018-10 fiscal: 196,986
---2018-09 fiscal: 197,698
SUM(IF(visit__application_details__application_name = 'SpecNet'
   AND message__name = 'selectAction'
   AND state__view__current_page__elements__standardized_name = 'pay-bill.billing-statement-download', 1, 0)) AS view_statements,

--One-Time Payments (OTP) (**NOTE** This is combined visits that did OTP + visits that signed up for AP during payment)
---2018-11 fiscal: 1,153,452
---2018-10 fiscal: 1,205,432 (Jake ETL is using the OLD DEFINITION Below. Provides a count of 1,205,706 diff of -0.023%)
---2018-09 fiscal: 1,143,224 (Changed definition to match KKellner)
---^^^With new definition: 1,142,943 (0.02% less)^^^
--OLD DEFINITION
--(SIZE(COLLECT_SET(IF (visit__application_details__application_name = 'SpecNet'
--            AND LOWER(message__name) = LOWER('featureStop')
--            AND LOWER(message__feature__feature_name) = LOWER('oneTimeBillPayFlow')
--            AND LOWER(operation__success) = LOWER('True')
--            AND LOWER(message__feature__feature_step_changed) = LOWER('False') , visit__visit_id, NULL)))
--+
--SIZE(COLLECT_SET(IF (visit__application_details__application_name = 'SpecNet'
--           AND LOWER(message__name) = LOWER('featureStop')
--           AND LOWER(message__feature__feature_name) = LOWER('oneTimeBillPayFlow')
--           AND LOWER(operation__success) = LOWER('True')
--           AND LOWER(message__feature__feature_step_name) = LOWER('oneTimePaymentAutoPaySuccess'), visit__visit_id, NULL)))) AS one_time_payments,
--NEW DEFINITION
SIZE(COLLECT_SET(IF(
 (
   (LOWER(visit__application_details__application_name) = 'myspectrum' AND message__name = 'pageView' AND state__view__current_page__page_name IN ('paySuccess','paySuccessAutoPay'))
   OR
   (LOWER(visit__application_details__application_name) = 'specnet' AND message__name = 'featureStop' AND message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE
     AND (message__feature__feature_step_changed = FALSE OR message__feature__feature_step_name = 'oneTimePaymentAutoPaySuccess'))
 ), visit__visit_id, NULL))) AS one_time_payments,

--Set-up Auto-Payments (AP) (**NOTE** This is combined visits that did AP Setup + visits that signed up for AP during payment)
---2018-11 fiscal: 63,145
---2018-10 fiscal: 67,631 (JMcCune Definition totalled 69,037 % diff of 2.03%)
---2018-09 fiscal: 74,008 (Changed definition to match KKellner)
---^^^With new definition: 72,578 (1.93% less)^^^
--OLD DEFINITION
--(SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('SpecNet')
--                AND LOWER(message__name) = LOWER('featureStop')
--                AND LOWER(message__feature__feature_name) IN (LOWER('autoPayEnrollment'),LOWER('manageAutoPay'))
--                AND LOWER(operation__success) = LOWER('True')
--                AND LOWER(message__feature__feature_step_name) <> LOWER('autopayChangesConfirmation'), visit__visit_id, NULL)))
--+
--SIZE(COLLECT_SET(IF (visit__application_details__application_name = 'SpecNet'
--                AND LOWER(message__name) = LOWER('featureStop')
--                AND LOWER(message__feature__feature_name) = LOWER('oneTimeBillPayFlow')
--                AND LOWER(operation__success) = LOWER('True')
--                AND LOWER(message__feature__feature_step_name) = LOWER('oneTimePaymentAutoPaySuccess'), visit__visit_id, NULL)))) AS set_up_auto_payments,
--NEW DEFINITION
SIZE(COLLECT_SET(IF(
  (
    (LOWER(visit__application_details__application_name) = 'myspectrum' AND message__name = 'pageView' AND state__view__current_page__page_name = 'autoPaySuccess')
    OR
    (LOWER(visit__application_details__application_name) = 'specnet' AND message__name = 'featureStop' AND operation__success = TRUE
      AND ((message__feature__feature_name IN ('autoPayEnrollment','manageAutopay') AND message__feature__feature_step_name <> 'autopayChangesConfirmation')
        OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND message__feature__feature_step_name = 'oneTimePaymentAutoPaySuccess')))
  ), visit__visit_id, NULL))) AS set_up_auto_payments,

--Total New IDs Created (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (numerator) divided by total attempts gets our % success)
---2018-10 fiscal:
---2018-09 fiscal ADOBE: 154,166
-- AS total_new_ids_created

--Total New ID Create Attempts (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (denominator) divided by total attempts gets our % success)
---2018-10 fiscal:
---2018-09 fiscal ADOBE: 494,158
-- AS total_new_id_creation_attempts

--Successful Password Resets (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (numerator) divided by total attempts gets our % success)
---2018-10 fiscal:
---2018-09 fiscal ADOBE: 238,353
-- AS successful_password_resets

--Password Reset Attempts (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (denominator) divided by total attempts gets our % success)
---2018-10 fiscal:
---2018-09 fiscal ADOBE: 326,422
-- AS password_reset_attempts

--Total Successful ID Recovery (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (numerator) divided by total attempts gets our % success)
---2018-10 fiscal:
---2018-09 fiscal ADOBE: 110,489
-- AS total_successful_id_recovery

--Total ID Recovery Attempts (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (denominator) divided by total attempts gets our % success)
---2018-10 fiscal:
---2018-09 fiscal ADOBE: 180,415
-- AS total_id_recovery_attempts

--Refresh Digital Receiver Requests (Counting instances of tv equipment reset FLOW starts)
---September report has 32,517 for the total. Doug couldn't back into the number.
---He tested counting instances, visits, and both by CHARTER MSO
---The final version of the excel has 23,706 in a column at the end titled "Storage | Old Calcs | Senseless Numbers"
---23,706 is the total visits for CHARTER mso and how other numbers were calculated
---Future reports will reflect the total instance counts
---However, using the counts.tsv and the metric listed (tv_equipment_reset_flow_starts) in the old prod monthly ETL provides the below number(s)
---2018-11 fiscal: 28,738
---2018-10 fiscal: 37,928
---2018-09 fiscal: 37,419
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('SpecNet')
   AND LOWER(message__name) = LOWER('selectAction')
   AND LOWER(state__view__current_page__elements__standardized_name) = 'equipment.tv.troubleshoot', 1, 0)) AS refresh_digital_receiver_requests,

--Modem Router Resets (Counting instances of internet equipment reset starts)
---September report has 35,052 for the total. << Incorrect way of calculating. It was counting visits for CHARTER MSO.
---...It SHOULD be instances for ALL MSO's
---Using the counts.tsv and the metric listed (internet_equipment_reset_flow_starts) provides the below number(s)
---2018-11 fiscal: 44,839
---2018-10 fiscal: 49,841
---2018-09 fiscal: 54,268
SUM(IF(
 (
   (LOWER(visit__application_details__application_name) = 'myspectrum' AND message__name = 'modalView'
     AND ((state__view__modal__name IN('internetresetsuccess-modal') AND state__view__previous_page__page_name IN ('internetTab','equipmentHome') AND state__view__current_page__page_name <> 'voiceTab')
       OR (state__view__modal__name = 'resetsuccess-modal')))
   OR
   (LOWER(visit__application_details__application_name) = 'specnet' AND message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'equipment.internet.troubleshoot')
 ), 1, 0)) AS modem_router_resets,

--Rescheduled Service Appointments (Counting instances of appointment reschedules)
---September report has 2,699 for the total. << Incorrect way of calculating. It was counting visits for CHARTER MSO.
---...It SHOULD be instances for ALL MSO's
---Using the counts.tsv and the metric listed (rescheduled_service_appointments) provides the below number(s)
---2018-11 fiscal: 2,511
---2018-10 fiscal: 2,566
---2018-09 fiscal: 3,388
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('SpecNet')
   AND LOWER(message__name) = LOWER('apiCall')
   AND LOWER(application__api__path) = LOWER('/api/pub/serviceapptedge/v1/appointments/reschedule'), 1, 0)) AS rescheduled_appointments,

--Canceled Service Appointments (Used to be displayed "Cancelled" but Doug has relented and gone back to Americanized spelling)
---September report has 2,362 for the total. << Incorrect way of calculating. It was counting visits for CHARTER MSO.
---...It SHOULD be instances for ALL MSO's
---Using the counts.tsv and the metric listed (cancelled_service_appointments) provides the below number(s)
---2018-11 fiscal: 2,141
---2018-10 fiscal: 2,587
---2018-09 fiscal: 2,557
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('SpecNet')
   AND LOWER(message__name) = LOWER('apiCall')
   AND LOWER(application__api__api_name) = LOWER('serviceapptedgeV1AppointmentsCancel'), 1, 0)) AS canceled_appointments,

'This concludes Quantum CHTR Resi Definitions' AS report_summary

FROM prod.asp_v_venona_events_portals_specnet
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')

;


-----------------------------------------------------------------------------------------
-- Adobe CHTR Resi Prod Monthly Definitions (Only needed to fulfill ID-Related metrics)
-----------------------------------------------------------------------------------------
SELECT

--Total New IDs Created (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (numerator) divided by total attempts gets our % success)
---2018-11 fiscal ADOBE: 121,488 (Ran this on 2018-11-22 and again on 2018-11-23 and numbers matched)
---2018-10 fiscal ADOBE: 133,099
---2018-09 fiscal ADOBE: 154,166
SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.bam','my-account.create-id-final.btm')
                AND message__category = 'Page View', visit__visit_id,NULL))) +
SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-final.nbtm')
                AND message__category = 'Page View', visit__visit_id,NULL))) AS ids_created,

--Total New ID Create Attempts (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (denominator) divided by total attempts gets our % success)
---2018-11 fiscal ADOBE: 421,767
---2018-10 fiscal ADOBE: 447,615
---2018-09 fiscal ADOBE: 494,158
SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-1.bam','my-account.create-id')
                AND message__category = 'Page View', visit__visit_id,NULL))) -
SIZE(COLLECT_SET(IF(message__name = 'Sign-In-Now'
                AND message__category = 'Custom Link'
                AND state__view__current_page__name IN('my-account.create-id-2.btm','my-account.create-id-2.bam'), visit__visit_id,NULL))) +
SIZE(COLLECT_SET(IF(state__view__current_page__name IN('my-account.create-id-1.nbtm')
                AND message__category = 'Page View', visit__visit_id,NULL))) -
SIZE(COLLECT_SET(IF(message__name = 'Sign-In-Now'
                AND message__category = 'Custom Link'
                AND state__view__current_page__name IN('my-account.create-id-2.nbtm'), visit__visit_id,NULL))) -
SIZE(COLLECT_SET(IF(state__view__current_page__name IN('Create.redirectTWC')
                AND message__category = 'Page View', visit__visit_id,NULL))) -
SIZE(COLLECT_SET(IF(state__view__current_page__name IN('Create.redirectBHN')
                AND message__category = 'Page View', visit__visit_id,NULL))) AS id_creation_attempts,

--Successful Password Resets (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (numerator) divided by total attempts gets our % success)
---2018-11 fiscal ADOBE: 220,103
---2018-10 fiscal ADOBE: 234,668
---2018-09 fiscal ADOBE: 238,353
----counts.tsv mirror
SIZE(COLLECT_SET(IF(message__category = 'Page View'
                AND state__view__current_page__name IN ('Reset-final.bam','Reset-final.btm')
                OR (message__category = 'Page View' AND state__view__current_page__name = 'Reset-final.nbtm'), visit__visit_id, NULL))) AS password_reset_success,

--Password Reset Attempts (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (denominator) divided by total attempts gets our % success)
---2018-11 fiscal ADOBE: 295,963
---2018-10 fiscal ADOBE: 325,757
---2018-09 fiscal ADOBE: 326,422
----counts.tsv mirror
SIZE(COLLECT_SET(IF(message__category = 'Page View'
                AND state__view__current_page__name IN('Reset-1.bam','Reset-1.btm')
                OR (message__category = 'Page View'
                AND state__view__current_page__name = 'Reset-1.nbtm'), visit__visit_id, NULL))) AS password_reset_attempts,

--Total Successful ID Recovery (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (numerator) divided by total attempts gets our % success)
---2018-11 fiscal ADOBE:  94,513
---2018-10 fiscal ADOBE: 110,155 (110,155 / 177,747 = 61.9729% Success)
---2018-09 fiscal ADOBE: 110,489 (110,489 / 180,415 = 61.2416% Success)
----counts.tsv mirror
SIZE(COLLECT_SET(if((state__view__current_page__name IN('Recover-final2.btm','Recover-final1.bam','Recover-final2.bam') AND message__category = 'Page View'),visit__visit_id,NULL))) +
  SIZE(COLLECT_SET(if((state__view__current_page__name = 'Recover-final1.nbtm') AND (message__category = 'Page View'), visit__visit_id,NULL))) +
  SIZE(COLLECT_SET(IF(state__view__current_page__name = 'Recover-final2.nbtm' AND (message__category = 'Page View'), visit__visit_id,NULL))) AS id_recovery_success,

--Total ID Recovery Attempts (**NO CURRENT DEF FOR QUANTUM UNTIL IDM RELEASE** This number (denominator) divided by total attempts gets our % success)
---2018-11 fiscal ADOBE: 161,565
---2018-10 fiscal ADOBE: 177,747
---2018-09 fiscal ADOBE: 180,415
----counts.tsv mirror
SIZE(COLLECT_SET(IF(message__category = 'Page View'
                AND state__view__current_page__name IN('Recover-1.btm','Recover-1.bam')
                OR  message__category = 'Page View'
                AND state__view__current_page__name = 'Recover-1.nbtm'
                AND state__view__current_page__name NOT IN  ('Recover-noID.nbtm'), visit__visit_id, NULL))) AS id_recovery_attempts,

'This concludes ADOBE CHTR Resi Definitions' AS report_summary


FROM prod.asp_v_net_events
WHERE (partition_date >= '2018-10-22' AND partition_date < '2018-11-22')
;


-----------------------------------------------------------------------------------------
-- Quantum CHTR Support Section Prod Monthly Definition
-----------------------------------------------------------------------------------------
--**NOTE** The below work was a shot in the dark to try and break out legacy company for support.
--NOT a good idea
SELECT
visit__application_details__application_name,
--visit__application_details__referrer_link,
CASE
  WHEN LOWER(visit__application_details__application_name) = 'specnet'
    AND visit__application_details__referrer_link RLIKE '.*brighthouse\.com.*' THEN 'BHN RESI'
  WHEN LOWER(visit__application_details__application_name) = 'smb'
    AND visit__application_details__referrer_link RLIKE '.*brighthouse\.com.*' THEN 'BHN SMB'
  WHEN LOWER(visit__application_details__application_name) = 'specnet'
    AND visit__application_details__referrer_link RLIKE '.*www\.timewarnercable\.com.*' THEN 'TWC RESI'
  WHEN LOWER(visit__application_details__application_name) = 'smb'
    AND visit__application_details__referrer_link RLIKE '.*business\.timewarnercable\.com.*' THEN 'TWC SMB'
  WHEN LOWER(visit__application_details__application_name) = 'specnet'
    AND visit__application_details__referrer_link RLIKE '.*spectrum\.net.*' THEN 'CHTR RESI'
  WHEN LOWER(visit__application_details__application_name) = 'specnet'
    AND visit__application_details__referrer_link RLIKE '.*spectrum\.com.*' THEN 'CHTR RESI'
  ELSE 'OTHER'
  END as referrer_grouping,




COUNT(1) as nbr_rows

FROM prod.asp_v_venona_events_portals
WHERE (partition_date_hour_utc >= '2018-09-22_06' AND partition_date_hour_utc < '2018-10-22_06')
AND LOWER(visit__application_details__application_name) IN('specnet','smb')
AND state__view__current_page__app_section = 'support'

GROUP BY
CASE
  WHEN LOWER(visit__application_details__application_name) = 'specnet'
    AND visit__application_details__referrer_link RLIKE '.*brighthouse\.com.*' THEN 'BHN RESI'
  WHEN LOWER(visit__application_details__application_name) = 'smb'
    AND visit__application_details__referrer_link RLIKE '.*brighthouse\.com.*' THEN 'BHN SMB'
  WHEN LOWER(visit__application_details__application_name) = 'specnet'
    AND visit__application_details__referrer_link RLIKE '.*www\.timewarnercable\.com.*' THEN 'TWC RESI'
  WHEN LOWER(visit__application_details__application_name) = 'smb'
    AND visit__application_details__referrer_link RLIKE '.*business\.timewarnercable\.com.*' THEN 'TWC SMB'
  WHEN LOWER(visit__application_details__application_name) = 'specnet'
    AND visit__application_details__referrer_link RLIKE '.*spectrum\.net.*' THEN 'CHTR RESI'
  WHEN LOWER(visit__application_details__application_name) = 'specnet'
    AND visit__application_details__referrer_link RLIKE '.*spectrum\.com.*' THEN 'CHTR RESI'
  ELSE 'OTHER'
  END,
visit__application_details__application_name
--visit__application_details__referrer_link
;
