set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_counts_hourly_counts_instances_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_counts_hourly_counts_instances_columns AS
    SELECT
        'L-TWCC' as company,
        SUM(IF( message__name IN ('MyTWC > CU > Ask TWC > Agent Answer'), 1, 0)) AS ask_charter,
        SUM(IF( message__name IN ( 'AutoPay > Setup > New Credit Card or Debit Card > Error','AutoPay > Setup > Saved Card > Error','AutoPay > Setup > Bank Account > Error','AutoPay > Setup > Saved EFT > Error','AutoPay > Setup > Debit Card > Error','AutoPay > Setup > Credit Card > Error')  AND message__category = 'Page View' , 1, 0)) AS autopay_enroll_failure,
        SUM(IF( message__name IN ( 'AutoPay > Setup > Payment Method > New Credit Card or Debit Card','AutoPay > Setup > Payment Method','AutoPay > Setup > Payment Method > Saved Card','AutoPay > Setup > Payment Method > Bank Account','AutoPay > Setup > Payment Method > Saved EFT','AutoPay > Setup > Payment Method > Credit Card','AutoPay > Setup > Payment Method > Debit Card')  AND message__category = 'Page View' , 1, 0)) AS autopay_enroll_start,
        SUM(IF( message__name IN ( 'AutoPay > Setup > New Credit Card or Debit Card > Success','AutoPay > Setup > Saved Card > Success','AutoPay > Setup > Bank Account > Success','AutoPay > Setup > Saved EFT > Success','AutoPay > Setup > Debit Card > Success','AutoPay > Setup > Credit Card > Success')  AND message__category = 'Page View' , 1, 0)) AS autopay_enroll_success,
        SUM(IF( message__name RLIKE '.*Contact(Now|Later)Success.*', 1, 0)) AS call_support,
        SUM(IF( message__name IN ('MyTWC > Appointment Manager > Canceled'), 1, 0)) AS cancelled_appointments,
        SUM(IF( message__name RLIKE 'MyTWC > Services & Troubleshoot > Equipment > Internet > Troubleshooting.*Connection'  OR message__name RLIKE 'MyTWC > Services & Troubleshoot > Equipment > Home Phone > Troubleshooting  ?(No Dial|Trouble making|Poor Call)', 1, 0)) AS modem_router_resets,
        SUM(IF( message__name IN ('MyTWC > Billing > Make Payment > Unsuccessful Payment')  AND message__category = 'Page View' , 1, 0)) AS one_time_payment_failure,
        SUM(IF( message__name IN ('MyTWC > Billing > Make Payment > Payment Confirmation')  AND message__category = 'Page View' , 1, 0)) AS one_time_payment_start,
        SUM(IF( message__name IN ('MyTWC > Billing > Make Payment > Successful Payment')  AND message__category = 'Page View' , 1, 0)) AS one_time_payment_success,
        SUM(IF( message__category IN ('Page View'), 1, 0)) AS page_views,
        SUM(IF( message__name RLIKE 'MyTWC > Services & Troubleshoot > Equipment > TV > Troubleshooting.*> Su', 1, 0)) AS refresh_digital_receiver_requests,
        SUM(IF( message__name IN ('MyTWC > Appointment Manager > Reschedule Complete'), 1, 0)) AS rescheduled_appointments,
        SUM(IF( TRUE, 1, 0)) AS site_unique,
        SUM(IF( message__name RLIKE 'MyTWC > Help.*', 1, 0)) AS support_page_views,
        SUM(IF( message__name IN ('AMACTION:MyTWC > Billing > Make Payment > Statement Download','AMACTION:MyTWC > Billing > Statement History > Statement Download'), 1, 0)) AS view_statement,
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
            'instances',
            'asp',
            'app',
            company,
            date_denver,
            'asp_v_twc_app_events'
    FROM (SELECT  company,
                  date_denver,
                  date_hour_denver,
                  MAP(

                      'ask_charter|Ask Charter Requests|MyTWC_adobe||', ask_charter,
                      'autopay_enroll_failure|AutoPay Enroll Failure|MyTWC_adobe||', autopay_enroll_failure,
                      'autopay_enroll_start|AutoPay Enroll Start|MyTWC_adobe||', autopay_enroll_start,
                      'autopay_enroll_success|AutoPay Enroll Success|MyTWC_adobe||', autopay_enroll_success,
                      'call_support|Call Support|MyTWC_adobe||', call_support,
                      'cancelled_appointments|Cancelled Appointments|MyTWC_adobe||', cancelled_appointments,
                      'modem_router_resets|Modem Router Resets|MyTWC_adobe||', modem_router_resets,
                      'one_time_payment_failure|One Time Payment Failure|MyTWC_adobe||', one_time_payment_failure,
                      'one_time_payment_start|One Time Payment Start|MyTWC_adobe||', one_time_payment_start,
                      'one_time_payment_success|One Time Payment Success|MyTWC_adobe||', one_time_payment_success,
                      'page_views|Page Views|MyTWC_adobe||', page_views,
                      'refresh_digital_receiver_requests|Refresh Digital Receiver Requests|MyTWC_adobe||', refresh_digital_receiver_requests,
                      'rescheduled_appointments|Rescheduled Appointments|MyTWC_adobe||', rescheduled_appointments,
                      'site_unique|Site Unique Values|MyTWC_adobe||', site_unique,
                      'support_page_views|Support Page Views|MyTWC_adobe||', support_page_views,
                      'view_statement|View Statement|MyTWC_adobe||', view_statement
                  ) as map_column
          FROM ${env:TMP_db}.asp_counts_hourly_counts_instances_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
