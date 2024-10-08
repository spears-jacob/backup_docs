set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_counts_hourly_counts_devices_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_counts_hourly_counts_devices_columns AS
    SELECT
        'L-TWCC' as company,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > CU > Ask TWC > Agent Answer'), visit__device__enc_uuid, Null))) AS ask_charter_devices,
        SIZE(COLLECT_SET(IF( message__name IN ( 'AutoPay > Setup > New Credit Card or Debit Card > Error','AutoPay > Setup > Saved Card > Error','AutoPay > Setup > Bank Account > Error','AutoPay > Setup > Saved EFT > Error','AutoPay > Setup > Debit Card > Error','AutoPay > Setup > Credit Card > Error')  AND message__category = 'Page View' , visit__device__enc_uuid, Null))) AS autopay_enroll_failure_devices,
        SIZE(COLLECT_SET(IF( message__name IN ( 'AutoPay > Setup > Payment Method > New Credit Card or Debit Card','AutoPay > Setup > Payment Method','AutoPay > Setup > Payment Method > Saved Card','AutoPay > Setup > Payment Method > Bank Account','AutoPay > Setup > Payment Method > Saved EFT','AutoPay > Setup > Payment Method > Credit Card','AutoPay > Setup > Payment Method > Debit Card')  AND message__category = 'Page View' , visit__device__enc_uuid, Null))) AS autopay_enroll_start_devices,
        SIZE(COLLECT_SET(IF( message__name IN ( 'AutoPay > Setup > New Credit Card or Debit Card > Success','AutoPay > Setup > Saved Card > Success','AutoPay > Setup > Bank Account > Success','AutoPay > Setup > Saved EFT > Success','AutoPay > Setup > Debit Card > Success','AutoPay > Setup > Credit Card > Success')  AND message__category = 'Page View' , visit__device__enc_uuid, Null))) AS autopay_enroll_success_devices,
        SIZE(COLLECT_SET(IF( message__name RLIKE '.*Contact(Now|Later)Success.*', visit__device__enc_uuid, Null))) AS call_support_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > Appointment Manager > Canceled'), visit__device__enc_uuid, Null))) AS cancelled_appointments_devices,
        SIZE(COLLECT_SET(IF( message__name RLIKE 'MyTWC > Services & Troubleshoot > Equipment > Internet > Troubleshooting.*Connection'  OR message__name RLIKE 'MyTWC > Services & Troubleshoot > Equipment > Home Phone > Troubleshooting  ?(No Dial|Trouble making|Poor Call)', visit__device__enc_uuid, Null))) AS modem_router_resets_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > Billing > Make Payment > Unsuccessful Payment')  AND message__category = 'Page View' , visit__device__enc_uuid, Null))) AS one_time_payment_failure_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > Billing > Make Payment > Payment Confirmation')  AND message__category = 'Page View' , visit__device__enc_uuid, Null))) AS one_time_payment_start_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > Billing > Make Payment > Successful Payment')  AND message__category = 'Page View' , visit__device__enc_uuid, Null))) AS one_time_payment_success_devices,
        SIZE(COLLECT_SET(IF( message__category IN ('Page View'), visit__device__enc_uuid, Null))) AS page_views_devices,
        SIZE(COLLECT_SET(IF( message__name RLIKE 'MyTWC > Services & Troubleshoot > Equipment > TV > Troubleshooting.*> Su', visit__device__enc_uuid, Null))) AS refresh_digital_receiver_requests_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > Appointment Manager > Reschedule Complete'), visit__device__enc_uuid, Null))) AS rescheduled_appointments_devices,
        SIZE(COLLECT_SET(IF( TRUE, visit__device__enc_uuid, Null))) AS site_unique_devices,
        SIZE(COLLECT_SET(IF( message__name RLIKE 'MyTWC > Help.*', visit__device__enc_uuid, Null))) AS support_page_views_devices,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:MyTWC > Billing > Make Payment > Statement Download','AMACTION:MyTWC > Billing > Statement History > Statement Download'), visit__device__enc_uuid, Null))) AS view_statement_devices,
        'asp' AS platform,
        'app' AS domain,
        prod.epoch_datehour(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_hour_denver,
        epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_denver
    FROM asp_v_twc_app_events
         
    WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
       AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
    GROUP BY
        epoch_converter(cast(message__timestamp * 1000 as bigint),'America/Denver'),
        prod.epoch_datehour(cast(message__timestamp * 1000 as bigint),'America/Denver'),
        'L-TWCC'
    ;
INSERT OVERWRITE TABLE prod.asp_counts_hourly
PARTITION(unit,platform,domain,company,date_denver,source_table)

    SELECT  value,
            metric,
            date_hour_denver,
            'devices',
            'asp',
            'app',
            company,
            date_denver,
            'asp_v_twc_app_events'
    FROM (SELECT  company,
                  date_denver,
                  date_hour_denver,
                  MAP(

                      'ask_charter|Ask Charter Requests|MyTWC_adobe||', ask_charter_devices,
                      'autopay_enroll_failure|AutoPay Enroll Failure|MyTWC_adobe||', autopay_enroll_failure_devices,
                      'autopay_enroll_start|AutoPay Enroll Start|MyTWC_adobe||', autopay_enroll_start_devices,
                      'autopay_enroll_success|AutoPay Enroll Success|MyTWC_adobe||', autopay_enroll_success_devices,
                      'call_support|Call Support|MyTWC_adobe||', call_support_devices,
                      'cancelled_appointments|Cancelled Appointments|MyTWC_adobe||', cancelled_appointments_devices,
                      'modem_router_resets|Modem Router Resets|MyTWC_adobe||', modem_router_resets_devices,
                      'one_time_payment_failure|One Time Payment Failure|MyTWC_adobe||', one_time_payment_failure_devices,
                      'one_time_payment_start|One Time Payment Start|MyTWC_adobe||', one_time_payment_start_devices,
                      'one_time_payment_success|One Time Payment Success|MyTWC_adobe||', one_time_payment_success_devices,
                      'page_views|Page Views|MyTWC_adobe||', page_views_devices,
                      'refresh_digital_receiver_requests|Refresh Digital Receiver Requests|MyTWC_adobe||', refresh_digital_receiver_requests_devices,
                      'rescheduled_appointments|Rescheduled Appointments|MyTWC_adobe||', rescheduled_appointments_devices,
                      'site_unique|Site Unique Values|MyTWC_adobe||', site_unique_devices,
                      'support_page_views|Support Page Views|MyTWC_adobe||', support_page_views_devices,
                      'view_statement|View Statement|MyTWC_adobe||', view_statement_devices
                  ) as map_column
          FROM ${env:TMP_db}.asp_counts_hourly_counts_devices_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
