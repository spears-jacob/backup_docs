-----------------------------------------------------------------------------------------
-- ADOBE App MySpectrum Prod Monthly Definitions (Only Useful prior to July 22, 2018)
-----------------------------------------------------------------------------------------

SELECT
--Split by MSO
---This is likely NOT needed in future reporting for App
---Future state of app report will display "MySpectrum" metrics and "MyTWC" metrics
--CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
--        WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
--        WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWC'
--        WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
--        ELSE 'UNDEFINED'
--     END AS legacy_footprint, --Comment this out if you don't need MSO

--Unique Visitors
---2018-07 fiscal: 864,057
---2018-07 fiscal MSO: CHTR 164,949 | TWC 244,348 | BHN 256,313 | Undefined 863,963
---2017-10 fiscal: 281,903
---2017-10 fiscal MSO: CHTR 32,520 | TWC  | BHN 180,088 | Undefined 281,871
SIZE(COLLECT_SET(visit__device__enc_uuid)) AS unique_visitors,

--Authenticated Sessions / Visits
---2018-07 fiscal: 2,158,189
---2018-07 fiscal MSO: CHTR 387,011 | TWC 586,938 | BHN 674,639 | Undefined 2,053,184
---2017-10 fiscal: 685,577
---2017-10 fiscal MSO: CHTR 63,160 | TWC  | BHN 481,953 | Undefined 676,869
SIZE(COLLECT_SET(visit__visit_id)) AS authenticated_sessions_visits,

--App Downloads
---2018-09 fiscal:
---2018-09 fiscal MSO: CHTR | TWC | BHN | Undefined
--AS app_downloads,

--Support Section
---2018-07 fiscal: 553,894
---2018-07 fiscal MSO: CHTR 93,452 | TWC 191,851 | BHN 268,562 | Undefined 29
---2017-10 fiscal: 363,826
---2017-10 fiscal MSO: CHTR 43,503 | TWC  | BHN 319,985 | Undefined 338
SIZE(COLLECT_LIST(CASE
                    WHEN message__name
                      IN('Support View',
                         'Passpoint Setup View',
                         'Tutorial Walkthrough First Use View',
                         'Channel Lineups',
                         'Moving Form NewAddress View',
                         'Program Your Remote',
                         'Locations Map',
                         'Terms And Conditions',
                         'AMACTION:Moving Trigger')
                    THEN visit__visit_id
                    ELSE NULL
                  END)) AS support_section,

--View Online Statements
---2018-07 fiscal: 170,207
---2018-07 fiscal MSO: CHTR 66,329 | TWC 103,276 | BHN 602 | Undefined 0
---2017-10 fiscal:  83,713
---2017-10 fiscal MSO: CHTR 20,530 | TWC  | BHN 63,183 | Undefined 0
SIZE(COLLECT_LIST(CASE
                    WHEN message__name
                      IN('AMACTION:PDFStatementDownload Svc Call Success',
                         'AMACTION:PDF Single Statement Svc Call Success')
                    THEN visit__visit_id
                    ELSE NULL
                   END)) AS view_online_statements,

--One Time Payment
---2018-07 fiscal: 325,013
---2018-07 fiscal MSO: CHTR 95,152 | TWC 73,137 | BHN 156,748 | Undefined 0
---2017-10 fiscal: 100,252
---2017-10 fiscal MSO: CHTR 11,430 | TWC  | BHN 88,831 | Undefined 0
SIZE(COLLECT_SET(CASE
                  WHEN message__name IN('AMACTION:MakeOneTimePayment Svc Call Success')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS one_time_payment,

--Set-Up AutoPay
---2018-07 fiscal: 2,171
---2018-07 fiscal MSO: CHTR 0 | TWC 0 | BHN 2,171 | Undefined 0
---2017-10 fiscal: 1,231
---2017-10 fiscal MSO: CHTR 12 | TWC  | BHN 1,221 | Undefined 0
SIZE(COLLECT_SET(CASE
                  WHEN message__name IN('AMACTION:AddAutoPay Svc Call Success')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS set_up_autopay,

--Forgot Password Success
---2018-07 fiscal: 9,992
---2018-07 fiscal MSO: CHTR 1 | TWC 3 | BHN 1,314 | Undefined 8,709
---2017-10 fiscal: 5,104
---2017-10 fiscal MSO: CHTR 6 | TWC  | BHN 875 | Undefined 4,252
SIZE(COLLECT_SET(CASE
                  WHEN message__name IN('AMACTION:ForgotPasswordStep3 Svc Call Success')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS forgot_password_success,

--Forgot Username Success
---2018-07 fiscal: 11,337
---2018-07 fiscal MSO: CHTR 3 | TWC 7 | BHN 1,211 | Undefined 10,134
---2017-10 fiscal:  6,908
---2017-10 fiscal MSO: CHTR 6 | TWC  | BHN 818 | Undefined 6,094
SIZE(COLLECT_SET(CASE
                  WHEN message__name in ('AMACTION:ForgotUsername Svc Call Success')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS forgot_username_success,

--Modem Router Resets
---2018-07 fiscal: 25,151
---2018-07 fiscal MSO: CHTR 7,932 | TWC 0 | BHN 17,216 | Undefined 3
---2017-10 fiscal: 12,433
---2017-10 fiscal MSO: CHTR 0 | TWC  | BHN 12,433 | Undefined 0
SIZE(COLLECT_LIST(CASE
                    WHEN message__name
                      IN ('AMACTION:RebootEquipment Svc Call Success',
                          'AMACTION:Equipment Internet reboot Modem success',
                          'AMACTION:Equipment Internet reboot Router success'
                          )
                    THEN visit__visit_id
                    ELSE NULL
                  END)) AS modem_router_resets,

--Refresh Digital Receiver Requests
---2018-07 fiscal: 75,258
---2018-07 fiscal MSO: CHTR 0 | TWC 0 | BHN 75,253 | Undefined 5
---2017-10 fiscal: 46,289
---2017-10 fiscal MSO: CHTR 0 | TWC  | BHN 46,289 | Undefined 0
SIZE(COLLECT_LIST(CASE
                    WHEN message__name in ('AMACTION:RefreshEquipment Svc Call Success')
                    THEN visit__visit_id
                    ELSE NULL
                  END)) AS refresh_digital_receiver_requests,

--Canceled Appointments
---2018-07 fiscal: 569
---2018-07 fiscal MSO: CHTR 0 | TWC 0 | BHN 569 | Undefined 0
---2017-10 fiscal: 313
---2017-10 fiscal MSO: CHTR 0 | TWC  | BHN 313 | Undefined 0
SIZE(COLLECT_SET(CASE
                  WHEN message__name in ('AMACTION:CancelAppointment Svc Call Success')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS canceled_appointments,

--Rescheduled Service Appointments
---2018-07 fiscal: 1
---2018-07 fiscal MSO: CHTR 0 | TWC 0 | BHN 1 | Undefined 0
---2017-10 fiscal: 115
---2017-10 fiscal MSO: CHTR 0 | TWC  | BHN 115 | Undefined 0
 SIZE(COLLECT_SET(CASE
                    WHEN message__name in ('AMACTION:RescheduleAppointment Svc Call Success')
                    THEN visit__visit_id
                    ELSE NULL
                  END)) AS rescheduled_service_appointments,

--Ask Charter Requests
 --AS ask_charter_requests,

--Call Support or Request Callback
---2018-07 fiscal: 82,183
---2018-07 fiscal MSO: CHTR 16,614 | TWC 25,074 | BHN 40,497 | Undefined 0
---2017-10 fiscal: 29,237
---2017-10 fiscal MSO: CHTR 4,314 | TWC  | BHN 24,923 | Undefined 0
SIZE(COLLECT_SET(CASE
                  WHEN message__name IN('AMACTION:Call Support Trigger')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS call_support_or_request_callback,

'This concludes ADOBE MySpectrum APP Definitions' AS report_summary


FROM prod.asp_v_spc_app_events
WHERE (partition_date_hour_utc >= '2018-06-22_06' AND partition_date_hour_utc < '2018-07-22_06')

--GROUP BY
--CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
--        WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
--        WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWC'
--        WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
--        ELSE 'UNDEFINED'
--     END --Comment this out if you don't need MSO
;

-----------------------------------------------------------------------------------------
-- AppFigures App Prod Monthly Definitions
-----------------------------------------------------------------------------------------


SELECT
--2018-10 fiscal: See Quantum hql for history
'App Downloads' AS metric,
SUM(daily_downloads) as value,
CASE WHEN product_name = 'My BHN' THEN 'BHN'
     WHEN product_name = 'My TWC®' THEN 'TWCC'
     WHEN product_name = 'My Spectrum' THEN 'CHTR'
     ELSE 'UNDEFINED'
END as company,
'This concludes AppFigures APP Definitions' AS report_summary

FROM prod.asp_v_app_figures_downloads

WHERE (partition_date_denver >= '2018-09-22' AND (partition_date_denver < '2018-10-22' ))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
AND daily_downloads IS NOT NULL
GROUP BY
  product_name;
