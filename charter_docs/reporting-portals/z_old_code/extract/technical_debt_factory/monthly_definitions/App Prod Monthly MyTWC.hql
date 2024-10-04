-----------------------------------------------------------------------------------------
-- ADOBE App MyTWC Prod Monthly Definitions
-----------------------------------------------------------------------------------------

SELECT

--Unique Visitors **Only tracking Overall
---2018-11 fiscal:   841,820 (842,034) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal:   874,750
---2018-09 fiscal:   915,617
---2018-08 fiscal:   982,903
---2018-07 fiscal: 1,072,221
---2017-10 fiscal: 1,121,893
---2017-09 fiscal: 1,120,044

SIZE(COLLECT_SET(visit__device__enc_uuid)) AS unique_visitors,

--Authenticated Sessions / Visits **Only tracking Overall
---2018-11 fiscal: 2,912,915 (2,915,163) Ran this on 11-22-18 (left) AND 11-23-18 (right)
---2018-10 fiscal: 2,950,684
---2018-09 fiscal: 3,017,808
---2018-08 fiscal: 3,199,708
---2018-07 fiscal: 3,298,353
---2017-10 fiscal: 3,312,492
---2017-09 fiscal: 3,330,737
SIZE(COLLECT_SET(visit__visit_id)) AS web_sessions_visits,

--App Downloads (See Below)
---2018-09 fiscal:
--AS app_downloads,

--Support Section
---^^^Sep and Aug numbers got swapped in report with TWC ONLY. ^^^
---2018-10 fiscal: 690,100
---2018-09 fiscal: 607,178
---2018-08 fiscal: 618,987
---2018-07 fiscal: 724,465
---2017-10 fiscal: 816,987
---2017-09 fiscal: 1,014,289
SIZE(COLLECT_LIST(CASE WHEN (message__name RLIKE ('MyTWC > Help.*')
                             AND message__category = 'Page View' )THEN visit__visit_id ELSE NULL END)) AS support_section_page_views,

--View Online Statements
---2018-10 fiscal: 134,723
---2018-09 fiscal: 136,080
---2018-08 fiscal: 150,206
---2018-07 fiscal: 148,314
---2017-10 fiscal: 196,225
---2017-09 fiscal: 211,808
SIZE(COLLECT_LIST(CASE WHEN message__name in ('AMACTION:MyTWC > Billing > Make Payment > Statement Download',
                                                  'AMACTION:MyTWC > Billing > Statement History > Statement Download')
                           THEN visit__visit_id ELSE NULL END)) AS view_statements,

--One Time Payment
---2018-10 fiscal: 571,530
---2018-09 fiscal: 600,737
---2018-08 fiscal: 609,585
---2018-07 fiscal: 630,404
---2017-10 fiscal: 668,079
---2017-09 fiscal: 653,417
SIZE(COLLECT_SET(CASE WHEN message__name in ('MyTWC > Billing > Make Payment > Successful Payment') THEN visit__visit_id ELSE NULL END)) AS one_time_payments,

--Set-Up AutoPay
---2018-10 fiscal: 3,407
---2018-09 fiscal: 3,704
---2018-08 fiscal: 3,981
---2018-07 fiscal: 4,254
---2017-10 fiscal: 7,360
---2017-09 fiscal: 7,897
SIZE(COLLECT_SET(CASE
                  WHEN message__name in ( 'AutoPay > Setup > New Credit Card or Debit Card > Success',
                                                  'AutoPay > Setup > Saved Card > Success',
                                                  'AutoPay > Setup > Bank Account > Success',
                                                  'AutoPay > Setup > Saved EFT > Success',
                                                  'AutoPay > Setup > Debit Card > Success',
                                                  'AutoPay > Setup > Credit Card > Success')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS set_up_auto_payments,

--Forgot Password Success (no definition)
 --AS forgot_password_success,

--Forgot Username Success (no definition)
 --AS forgot_username_success,

--Modem Router Resets (We blanked out BHN in the report)
---2018-10 fiscal: 77,780
---2018-09 fiscal: 67,596
---2018-08 fiscal: 72,407
---2018-07 fiscal: 72,485
---2017-10 fiscal: 92,512
---2017-09 fiscal: 103,367
SIZE(COLLECT_SET(CASE
                  WHEN (message__name RLIKE ('MyTWC > Services & Troubleshoot > Equipment > Internet > Troubleshooting.*Connection')
                        OR message__name RLIKE ('MyTWC > Services & Troubleshoot > Equipment > Home Phone > Troubleshooting  ?(No Dial|Trouble making|Poor Call)')
                        )
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS modem_router_resets,

--Refresh Digital Receiver Requests
---2018-10 fiscal: 27,044
---2018-09 fiscal: 23,666
---2018-08 fiscal: 29,629
---2018-07 fiscal: 31,217
---2017-10 fiscal: 37,413
---2017-09 fiscal: 39,706
 SIZE(COLLECT_SET(CASE
                    WHEN message__name RLIKE ('MyTWC > Services & Troubleshoot > Equipment > TV > Troubleshooting.*> Su')
                    THEN visit__visit_id
                    ELSE NULL
                  END)) AS refresh_digital_receiver_requests,

--Canceled Appointments
---2018-10 fiscal: 2,187
---2018-09 fiscal: 2,015
---2018-08 fiscal: 2,464
---2018-07 fiscal: 2,348
---2017-10 fiscal: 3,014
---2017-09 fiscal: 3,362
SIZE(COLLECT_SET(CASE
                  WHEN message__name in ('MyTWC > Appointment Manager > Canceled')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS canceled_appointments,

--Rescheduled Service Appointments
---2018-10 fiscal: 1,152
---2018-09 fiscal: 1,189
---2018-08 fiscal: 1,244
---2018-07 fiscal: 1,324
---2017-10 fiscal: 2,158
---2017-09 fiscal: 2,441
SIZE(COLLECT_SET(CASE
                  WHEN message__name in ('MyTWC > Appointment Manager > Reschedule Complete')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS rescheduled_appointments,

--Ask Charter Requests
---2018-10 fiscal: 167
---2018-09 fiscal: 225
---2018-08 fiscal: 214
---2018-07 fiscal: 260
---2017-10 fiscal: 15,700
---2017-09 fiscal: 20,646
SIZE(COLLECT_SET(CASE
                  WHEN message__name in ('MyTWC > CU > Ask TWC > Agent Answer')
                  THEN visit__visit_id
                  ELSE NULL
                 END)) AS ask_charter_requests,

--Call Support or Request Callback
---2018-10 fiscal: 25,131
---2018-09 fiscal: 25,285
---2018-08 fiscal: 36,495
---2018-07 fiscal: 41,196
---2017-10 fiscal: 47,183
---2017-09 fiscal: 43,857
SIZE(COLLECT_LIST(CASE
                    WHEN message__name RLIKE '.*Contact(Now|Later)Success.*'
                    THEN visit__visit_id
                    ELSE NULL
                  END)) AS call_support_or_request_callback,

'This concludes ADOBE MyTWC APP Definitions' AS report_summary

FROM prod.asp_v_twc_app_events
WHERE (partition_date_hour_utc >= '2018-10-22_06' AND partition_date_hour_utc < '2018-11-22_06')

;

-----------------------------------------------------------------------------------------
-- AppFigures App Prod Monthly Definitions
-----------------------------------------------------------------------------------------


SELECT
--2018-09 fiscal: CHTR 293,507 | TWC 60,254
--2018-08 fiscal: CHTR 280,633 | TWC 62,588
'App Downloads' AS metric,
SUM(daily_downloads) as value,
CASE WHEN product_name = 'My BHN' THEN 'BHN'
     WHEN product_name = 'My TWC®' THEN 'TWCC'
     WHEN product_name = 'My Spectrum' THEN 'CHTR'
     ELSE 'UNDEFINED'
END as company,
'This concludes AppFigures APP Definitions' AS report_summary

FROM prod.asp_v_app_figures_downloads

WHERE (partition_date_denver >= '2018-07-22' AND (partition_date_denver < '2018-08-22' ))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
AND daily_downloads IS NOT NULL
GROUP BY
  product_name;
