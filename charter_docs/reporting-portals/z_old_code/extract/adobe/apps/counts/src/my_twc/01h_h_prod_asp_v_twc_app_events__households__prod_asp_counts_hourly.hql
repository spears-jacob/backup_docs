set hive.vectorized.execution.enabled = false;

USE prod;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_counts_hourly_counts_households_columns;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_counts_hourly_counts_households_columns AS
    SELECT
        'L-TWCC' as company,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > CU > Ask TWC > Agent Answer'), visit__account__enc_account_number, Null))) AS ask_charter_households,
        SIZE(COLLECT_SET(IF( message__name IN ( 'AutoPay > Setup > New Credit Card or Debit Card > Error','AutoPay > Setup > Saved Card > Error','AutoPay > Setup > Bank Account > Error','AutoPay > Setup > Saved EFT > Error','AutoPay > Setup > Debit Card > Error','AutoPay > Setup > Credit Card > Error')  AND message__category = 'Page View' , visit__account__enc_account_number, Null))) AS autopay_enroll_failure_households,
        SIZE(COLLECT_SET(IF( message__name IN ( 'AutoPay > Setup > Payment Method > New Credit Card or Debit Card','AutoPay > Setup > Payment Method','AutoPay > Setup > Payment Method > Saved Card','AutoPay > Setup > Payment Method > Bank Account','AutoPay > Setup > Payment Method > Saved EFT','AutoPay > Setup > Payment Method > Credit Card','AutoPay > Setup > Payment Method > Debit Card')  AND message__category = 'Page View' , visit__account__enc_account_number, Null))) AS autopay_enroll_start_households,
        SIZE(COLLECT_SET(IF( message__name IN ( 'AutoPay > Setup > New Credit Card or Debit Card > Success','AutoPay > Setup > Saved Card > Success','AutoPay > Setup > Bank Account > Success','AutoPay > Setup > Saved EFT > Success','AutoPay > Setup > Debit Card > Success','AutoPay > Setup > Credit Card > Success')  AND message__category = 'Page View' , visit__account__enc_account_number, Null))) AS autopay_enroll_success_households,
        SIZE(COLLECT_SET(IF( message__name RLIKE '.*Contact(Now|Later)Success.*', visit__account__enc_account_number, Null))) AS call_support_households,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > Appointment Manager > Canceled'), visit__account__enc_account_number, Null))) AS cancelled_appointments_households,
        SIZE(COLLECT_SET(IF( message__name RLIKE 'MyTWC > Services & Troubleshoot > Equipment > Internet > Troubleshooting.*Connection'  OR message__name RLIKE 'MyTWC > Services & Troubleshoot > Equipment > Home Phone > Troubleshooting  ?(No Dial|Trouble making|Poor Call)', visit__account__enc_account_number, Null))) AS modem_router_resets_households,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > Billing > Make Payment > Unsuccessful Payment')  AND message__category = 'Page View' , visit__account__enc_account_number, Null))) AS one_time_payment_failure_households,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > Billing > Make Payment > Payment Confirmation')  AND message__category = 'Page View' , visit__account__enc_account_number, Null))) AS one_time_payment_start_households,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > Billing > Make Payment > Successful Payment')  AND message__category = 'Page View' , visit__account__enc_account_number, Null))) AS one_time_payment_success_households,
        SIZE(COLLECT_SET(IF( message__category IN ('Page View'), visit__account__enc_account_number, Null))) AS page_views_households,
        SIZE(COLLECT_SET(IF( message__name RLIKE 'MyTWC > Services & Troubleshoot > Equipment > TV > Troubleshooting.*> Su', visit__account__enc_account_number, Null))) AS refresh_digital_receiver_requests_households,
        SIZE(COLLECT_SET(IF( message__name IN ('MyTWC > Appointment Manager > Reschedule Complete'), visit__account__enc_account_number, Null))) AS rescheduled_appointments_households,
        SIZE(COLLECT_SET(IF( TRUE, visit__account__enc_account_number, Null))) AS site_unique_households,
        SIZE(COLLECT_SET(IF( message__name RLIKE 'MyTWC > Help.*', visit__account__enc_account_number, Null))) AS support_page_views_households,
        SIZE(COLLECT_SET(IF( message__name IN ('AMACTION:MyTWC > Billing > Make Payment > Statement Download','AMACTION:MyTWC > Billing > Statement History > Statement Download'), visit__account__enc_account_number, Null))) AS view_statement_households,
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
            'households',
            'asp',
            'app',
            company,
            date_denver,
            'asp_v_twc_app_events'
    FROM (SELECT  company,
                  date_denver,
                  date_hour_denver,
                  MAP(

                      'ask_charter|Ask Charter Requests|MyTWC_adobe||', ask_charter_households,
                      'autopay_enroll_failure|AutoPay Enroll Failure|MyTWC_adobe||', autopay_enroll_failure_households,
                      'autopay_enroll_start|AutoPay Enroll Start|MyTWC_adobe||', autopay_enroll_start_households,
                      'autopay_enroll_success|AutoPay Enroll Success|MyTWC_adobe||', autopay_enroll_success_households,
                      'call_support|Call Support|MyTWC_adobe||', call_support_households,
                      'cancelled_appointments|Cancelled Appointments|MyTWC_adobe||', cancelled_appointments_households,
                      'modem_router_resets|Modem Router Resets|MyTWC_adobe||', modem_router_resets_households,
                      'one_time_payment_failure|One Time Payment Failure|MyTWC_adobe||', one_time_payment_failure_households,
                      'one_time_payment_start|One Time Payment Start|MyTWC_adobe||', one_time_payment_start_households,
                      'one_time_payment_success|One Time Payment Success|MyTWC_adobe||', one_time_payment_success_households,
                      'page_views|Page Views|MyTWC_adobe||', page_views_households,
                      'refresh_digital_receiver_requests|Refresh Digital Receiver Requests|MyTWC_adobe||', refresh_digital_receiver_requests_households,
                      'rescheduled_appointments|Rescheduled Appointments|MyTWC_adobe||', rescheduled_appointments_households,
                      'site_unique|Site Unique Values|MyTWC_adobe||', site_unique_households,
                      'support_page_views|Support Page Views|MyTWC_adobe||', support_page_views_households,
                      'view_statement|View Statement|MyTWC_adobe||', view_statement_households
                  ) as map_column
          FROM ${env:TMP_db}.asp_counts_hourly_counts_households_columns
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
