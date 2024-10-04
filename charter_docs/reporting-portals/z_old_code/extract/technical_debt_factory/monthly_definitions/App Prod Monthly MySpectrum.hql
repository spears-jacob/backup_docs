-----------------------------------------------------------------------------------------
-- Quantum App MySpectrum Prod Monthly Definitions
-----------------------------------------------------------------------------------------

SELECT
--Split by MSO
---This is likely NOT needed in future reporting for App
---Future state of app report will display "MySpectrum" metrics and "MyTWC" metrics
--visit__account__details__mso AS legacy_footprint, --Comment this out if you don't need MSO

--Unique Visitors
---2018-11 fiscal: 1,214,856 (Ran this on 2018-11-22 and again on 2018-11-23 and numbers matched)
---2018-11 fiscal MSO: CHTR 229,704 | TWC 426,024 | BHN 298,174 | Undefined 1,214,823
---2018-10 fiscal: 1,138,360
---2018-10 fiscal MSO: CHTR 217,822 | TWC 369,284 | BHN 293,726 | Undefined 1,138,334
---2018-09 fiscal: 1,113,308
---2018-09 fiscal MSO: CHTR 213,602 | TWC 362,466 | BHN 302,627 | Undefined 1,112,616
---2018-08 fiscal: 964,125
---2018-08 fiscal MSO: CHTR 187,280 | TWC 304,300 | BHN 279,617 | Undefined 964,088
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SIZE(COLLECT_SET(visit__device__enc_uuid)) AS unique_visitors,

--Authenticated Sessions / Visits
---2018-11 fiscal: 3,424,149
---2018-11 fiscal MSO: CHTR 588,988 | TWC 1,108,007 | BHN 860,942 | Undefined 3,423,899
---2018-10 fiscal: 3,180,100
---2018-10 fiscal MSO: CHTR 556,802 | TWC 923,689 | BHN 860,100 | Undefined 3,179,898
---2018-09 fiscal: 3,403,772
---2018-09 fiscal MSO: CHTR 523,951 | TWC 860,522 | BHN 1,063,919 | Undefined 3,395,823
---2018-08 fiscal: 2,646,683
---2018-08 fiscal MSO: CHTR 467,079 | TWC 689,146 | BHN 846,577 | Undefined 2,646,424
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SIZE(COLLECT_SET(visit__visit_id)) AS web_sessions_visits,

--App Downloads
---2018-09 fiscal:
---2018-09 fiscal MSO: CHTR | TWC | BHN | Undefined
--AS app_downloads,

--Support Section
---^^^Sep and Aug numbers got swapped in report with TWC ONLY. ^^^
---2018-11 fiscal: 912,787
---2018-11 fiscal MSO: CHTR 170,759 | TWC 423,307 | BHN 275,348 | Undefined 43,373
---2018-10 fiscal: 853,565
---2018-10 fiscal MSO: CHTR 177,875 | TWC 354,705 | BHN 277,365 | Undefined 43,620
---2018-09 fiscal: 922,436
---2018-09 fiscal MSO: CHTR 180,423 | TWC 368,119 | BHN 328,358 | Undefined 45,536
---2018-08 fiscal: 853,245
---2018-08 fiscal MSO: CHTR 157,445 | TWC 341,214  | BHN 315,885 | Undefined 38,701
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SUM(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('pageView')
    AND LOWER(state__view__current_page__app_section) = LOWER('support') , 1, 0)) AS support_section_page_views,

--View Online Statements
---2018-11 fiscal: 421,823
---2018-11 fiscal MSO: CHTR 118,870 | TWC 191,387 | BHN 97,364 | Undefined 14,202
---2018-10 fiscal: 359,417
---2018-10 fiscal MSO: CHTR 105,450 | TWC 149,540 | BHN 91,761 | Undefined 12,666
---2018-09 fiscal: 332,027
---2018-09 fiscal MSO: CHTR 99,469 | TWC 132,121 | BHN 87,871 | Undefined 12,566
---2018-08 fiscal: 297,224
---2018-08 fiscal MSO: CHTR 89,797 | TWC 110,307 | BHN 86,644 | Undefined 10,476
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
   AND LOWER(message__name) = LOWER('pageView')
   AND LOWER(state__view__current_page__page_name) = LOWER('statementDetail'), 1, 0)) AS view_statements,

--One Time Payment
---2018-11 fiscal: 533,344
---2018-11 fiscal MSO: CHTR 139,804 | TWC 193,404 | BHN 186,225 | Undefined 13,966
---2018-10 fiscal: 444,207
---2018-10 fiscal MSO: CHTR 117,252 | TWC 144,935 | BHN 170,116 | Undefined 11,953
---2018-09 fiscal: 432,181
---2018-09 fiscal MSO: CHTR 118,708| TWC 131,572 | BHN 169,707 | Undefined 12,249
---2018-08 fiscal: 383,396
---2018-08 fiscal MSO: CHTR 106,888 | TWC 106,031 | BHN 158,590 | Undefined 11,918
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SIZE(COLLECT_SET(IF(
  (
    (LOWER(visit__application_details__application_name) = 'myspectrum' AND message__name = 'pageView' AND state__view__current_page__page_name IN ('paySuccess','paySuccessAutoPay'))
    OR
    (LOWER(visit__application_details__application_name) = 'specnet' AND message__name = 'featureStop' AND message__feature__feature_name = 'oneTimeBillPayFlow' AND operation__success = TRUE
      AND (message__feature__feature_step_changed = FALSE OR message__feature__feature_step_name = 'oneTimePaymentAutoPaySuccess'))
  ), visit__visit_id, NULL))) AS one_time_payments,

--Set-Up AutoPay
---2018-11 fiscal: 2,935
---2018-11 fiscal MSO: CHTR 769 | TWC 1,965 | BHN 136 | Undefined 74
---2018-10 fiscal: 2,684
---2018-10 fiscal MSO: CHTR 702 | TWC 1,721 | BHN 179 | Undefined 87
---2018-09 fiscal: 4,056
---2018-09 fiscal MSO: CHTR 972 | TWC 2,007 | BHN 954 | Undefined 132
---2018-08 fiscal: 1,888
---2018-08 fiscal MSO: CHTR 675 | TWC 1,170 | BHN  | Undefined 45
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SIZE(COLLECT_SET(IF(
  (
    (LOWER(visit__application_details__application_name) = 'myspectrum' AND message__name = 'pageView' AND state__view__current_page__page_name = 'autoPaySuccess')
    OR
    (LOWER(visit__application_details__application_name) = 'specnet' AND message__name = 'featureStop' AND operation__success = TRUE
      AND ((message__feature__feature_name IN ('autoPayEnrollment','manageAutopay') AND message__feature__feature_step_name <> 'autopayChangesConfirmation')
        OR (message__feature__feature_name = 'oneTimeBillPayFlow' AND message__feature__feature_step_name = 'oneTimePaymentAutoPaySuccess')))
  ), visit__visit_id, NULL))) AS set_up_autopayments,

--Forgot Password Success

---2018-11 fiscal: 14,489
---2018-11 fiscal MSO: CHTR 1 | TWC 1 | BHN 286 | Undefined 14,212
---2018-10 fiscal: 14,120
---2018-10 fiscal MSO: CHTR 1 | TWC 0 | BHN 301 | Undefined 13,832
---2018-09 fiscal: 17,701  < - This was in the report... not the MSO split nbrs
---2018-09 fiscal MSO: CHTR 1 | TWC 2 | BHN 466 | Undefined 17,250
---2018-08 fiscal: 12,557
---2018-08 fiscal MSO: CHTR 0 | TWC 0 | BHN 268 | Undefined 12,297
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
                AND LOWER(message__name) = LOWER('modalView')
                AND LOWER(state__view__modal__name) = LOWER('newpasswordresponse-modal')
                AND LOWER(state__view__current_page__page_name) = LOWER('newPasswordEntry'), visit__visit_id, NULL))) AS password_reset_success,

--Forgot Username Success
---2018-11 fiscal: 16,211
---2018-11 fiscal MSO: CHTR 0 | TWC 4 | BHN 320 | Undefined 15,897
---2018-10 fiscal: 17,547
---2018-10 fiscal MSO: CHTR 4 | TWC 4 | BHN 454 | Undefined 17,109
---2018-09 fiscal: 22,122 < - This was in the report... not the MSO split nbrs
---2018-09 fiscal MSO: CHTR 1 | TWC 7 | BHN 1,013 | Undefined 21,130
---2018-08 fiscal: 14,967
---2018-08 fiscal MSO: CHTR 1 | TWC 0 | BHN 260 | Undefined 14,719
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SIZE(COLLECT_SET(IF(LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
                AND LOWER(message__name) = LOWER('selectAction')
                AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('loginRecoverySendEmail'), visit__visit_id, NULL))) AS id_recovery_success,

--Modem Router Resets (We blanked out BHN in the report)
---2018-11 fiscal: 33,982
---2018-11 fiscal MSO: CHTR 7,101 | TWC 24,871 | BHN 0 | Undefined 2,010
---2018-10 fiscal: 29,792
---2018-10 fiscal MSO: CHTR 8,314 | TWC 19,639 | BHN 49 | Undefined 1,790
---2018-09 fiscal: 22,496
---2018-09 fiscal MSO: CHTR 7,967 | TWC 13,030 | BHN 39 | Undefined 1,460
---2018-08 fiscal: 4,395
---2018-08 fiscal MSO: CHTR 4,206 | TWC 1 | BHN 4 | Undefined 184
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SUM(IF(
 (
   (LOWER(visit__application_details__application_name) = 'myspectrum' AND message__name = 'modalView'
     AND ((state__view__modal__name IN('internetresetsuccess-modal') AND state__view__previous_page__page_name IN ('internetTab','equipmentHome') AND state__view__current_page__page_name <> 'voiceTab')
       OR (state__view__modal__name = 'resetsuccess-modal')))
   OR
   (LOWER(visit__application_details__application_name) = 'specnet' AND message__name = 'selectAction' AND state__view__current_page__elements__standardized_name = 'equipment.internet.troubleshoot')
 ), 1, 0)) AS modem_router_resets,

--Refresh Digital Receiver Requests (Updating in Counts.tsv)
--^^^Sep-2018 is reflecting ONLY CHTR MSO for this number^^^
---2018-11 fiscal: 30,679
---2018-11 fiscal MSO: CHTR 6,860 | TWC 22,251 | BHN 0 | Undefined 1,568
---2018-10 fiscal: 33,430
---2018-10 fiscal MSO: CHTR 8,995 | TWC 22,532 | BHN 55 | Undefined 1,848
---2018-09 fiscal: 24,622
---2018-09 fiscal MSO: CHTR 6,230 | TWC 16,874 | BHN 60 | Undefined 1,460
---2018-08 fiscal: 22
---2018-08 fiscal MSO: CHTR 4 | TWC 3 | BHN 14 | Undefined 1
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SUM(IF(LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
   AND LOWER(message__name) = LOWER('selectAction')
   AND LOWER(state__view__current_page__page_name) = LOWER('tvTab')
   AND LOWER(state__view__current_page__elements__standardized_name)
          IN(LOWER('troubleshoot'),LOWER('internetConnectIssues'),LOWER('internetTroubleshoot')), 1, 0)) AS refresh_digital_receiver_requests,

--Canceled Appointments
---2018-11 fiscal: 631
---2018-11 fiscal MSO: CHTR 0 | TWC 0 | BHN 581 | Undefined 50
---2018-10 fiscal: 652
---2018-10 fiscal MSO: CHTR 0 | TWC 0 | BHN 598 | Undefined 54
---2018-09 fiscal: 739
---2018-09 fiscal MSO: CHTR 0 | TWC 0 | BHN 676 | Undefined 63
---2018-08 fiscal: 767
---2018-08 fiscal MSO: CHTR 0 | TWC 0 | BHN 709 | Undefined 59
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('modalView')
    AND LOWER(state__view__modal__name) = LOWER('cancelappointment-modal') , visit__visit_id, NULL))) AS canceled_appointments,

--Rescheduled Service Appointments
---2018-11 fiscal: 9
---2018-11 fiscal MSO: CHTR 0 | TWC 0 | BHN 9 | Undefined 0
---2018-10 fiscal: 6
---2018-10 fiscal MSO: CHTR 0 | TWC 0 | BHN 6 | Undefined 0
---2018-09 fiscal: 21
---2018-09 fiscal MSO: CHTR 0 | TWC 0 | BHN 19 | Undefined 2
---2018-08 fiscal: 4
---2018-08 fiscal MSO: CHTR 0 | TWC 0 | BHN 4 | Undefined 0
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('apiCall')
    AND LOWER(application__api__path) = LOWER('/care/api/v1/appointment/reschedule')
    AND application__api__response_code RLIKE '2.*' , visit__visit_id, NULL))) AS rescheduled_appointments,

--Ask Charter Requests
---2018-10 fiscal:
---2018-10 fiscal MSO: CHTR  | TWC | BHN  | Undefined
---2018-09 fiscal:
---2018-09 fiscal MSO: CHTR | TWC | BHN | Undefined
 --AS ask_charter_requests,

--Call Support or Request Callback
---2018-11 fiscal: 96,666
---2018-11 fiscal MSO: CHTR 16,836 | TWC 33,338 | BHN 39,810 | Undefined 6,689
---2018-10 fiscal: 106,022
---2018-10 fiscal MSO: CHTR 22,318 | TWC 37,111 | BHN 39,508 | Undefined 7,089
---2018-09 fiscal: 107,456
---2018-09 fiscal MSO: CHTR 20,745 | TWC 35,429 | BHN 43,761 | Undefined 8,921
---2018-08 fiscal: 103,713
---2018-08 fiscal MSO: CHTR 19,493 | TWC 32,802 | BHN 44,716 | Undefined 6,706
---2018-07 fiscal: SEE ADOBE DEFINITIONS
---2018-07 fiscal MSO: CHTR  | TWC  | BHN  | Undefined
SIZE(COLLECT_SET(IF( LOWER(visit__application_details__application_name) = LOWER('MySpectrum')
    AND LOWER(message__name) = LOWER('selectAction')
    AND LOWER(state__view__current_page__elements__standardized_name) = LOWER('callUsButton') , visit__visit_id, NULL))) AS call_support_or_request_callback,

'This concludes Quantum MySpectrum APP Definitions' AS report_summary


FROM prod.asp_v_venona_events_portals_msa
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')

--GROUP BY visit__account__details__mso --Comment this out if you don't need MSO
;

-----------------------------------------------------------------------------------------
-- AppFigures App Prod Monthly Definitions
-----------------------------------------------------------------------------------------


SELECT
--2018-11 fiscal: CHTR 265,868 | TWC 43,427  < < - - Pulled 2018-11-23 (MySpectrum 4.02% 1-day change | MyTWC 3.07% 1-day change)
--2018-11 fiscal: CHTR 255,603 | TWC 42,133  < < - - Pulled 2018-11-22
--2018-10 fiscal: CHTR 237,292 | TWC 48,074
--2018-09 fiscal: CHTR 293,507 | TWC 60,254
--2018-08 fiscal: CHTR 280,633 | TWC 62,588
--2017-12 fiscal: CHTR 139,341 | TWC 109,953
--2017-11 fiscal: CHTR 133,840 | TWC 125,681 | BHN NULL
--2017-10 fiscal: CHTR 104,715 | TWC 125,896 | BHN 12,790

'App Downloads' AS metric,
SUM(daily_downloads) as value,
CASE WHEN product_name = 'My BHN' THEN 'BHN'
     WHEN product_name = 'My TWC®' THEN 'TWCC'
     WHEN product_name = 'My Spectrum' THEN 'CHTR'
     ELSE 'UNDEFINED'
END as company,
'This concludes AppFigures APP Definitions' AS report_summary

FROM prod.asp_v_app_figures_downloads

WHERE (partition_date_denver >= '2018-10-22' AND (partition_date_denver < '2018-11-22' ))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
AND daily_downloads IS NOT NULL
GROUP BY
  product_name;
