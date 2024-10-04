USE ${env:ENVIRONMENT};

-- Queries started failing due to java.lang.OutOfMemoryError: Java heap space
set hive.auto.convert.join=false;

-- Insert OVERWRITE on first metric clears out partition for subsequent inserts

SELECT '\n\nNow running asp_app_agg_02c_my_twc...\n\n';

-- Page Views (uses insert overwrite to clear partition)
SELECT '\n\nNow selecting Page Views...\n\n';

INSERT OVERWRITE TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'Page Views' as metric,
    SIZE(COLLECT_LIST(case when message__category in ('Page View') THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Visits
SELECT '\n\nNow selecting Visits...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'Visits' as metric,
    SIZE(COLLECT_SET(visit__visit_id)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Unique Visitors
SELECT '\n\nNow selecting Unique Visitors...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'Unique Visitors' as metric,
    SIZE(COLLECT_SET(visit__device__enc_uuid)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Cancelled Appointments
SELECT '\n\nNow selecting Cancelled Appointments...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'Cancelled Appointments' as metric,
    SIZE(COLLECT_SET(case when message__name in ('MyTWC > Appointment Manager > Canceled') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Rescheduled Appointments
SELECT '\n\nNow selecting Rescheduled Appointments...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'Rescheduled Appointments' as metric,
    SIZE(COLLECT_SET(case when message__name in ('MyTWC > Appointment Manager > Reschedule Complete') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Ask Charter Requests (Chat initiated)
SELECT '\n\nNow selecting Ask Charter Requests ...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'Ask Charter Requests' as metric,
    SIZE(COLLECT_SET(case when message__name in ('MyTWC > CU > Ask TWC > Agent Answer') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Refresh Digital Receiver Requests
SELECT '\n\nNow selecting Refresh Digital Receiver Requests...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'Refresh Digital Receiver Requests' as metric,
    SIZE(COLLECT_SET(case when message__name RLIKE ('MyTWC > Services & Troubleshoot > Equipment > TV > Troubleshooting.*> Su') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Modem Router Resets
SELECT '\n\nNow selecting Modem Router Resets...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'Modem Router Resets' as metric,
    SIZE(COLLECT_SET(case when (    message__name RLIKE ('MyTWC > Services & Troubleshoot > Equipment > Internet > Troubleshooting.*Connection')
                                 OR message__name RLIKE ('MyTWC > Services & Troubleshoot > Equipment > Home Phone > Troubleshooting  ?(No Dial|Trouble making|Poor Call)') ) THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;


-- Support Page Views
SELECT '\n\nNow selecting Support Page Views...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'Support Page Views' as metric,
    SIZE(COLLECT_LIST(case when (message__name RLIKE ('MyTWC > Help.*')
                             AND message__category = 'Page View' )THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Call Support
SELECT '\n\nNow selecting Call Support...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'Call Support' as metric,
    SIZE(COLLECT_LIST(case when message__name RLIKE '.*Contact(Now|Later)Success.*' THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- View Statement
SELECT '\n\nNow selecting View Statement...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'View Statement' as metric,
    SIZE(COLLECT_LIST(case when message__name in ('AMACTION:MyTWC > Billing > Make Payment > Statement Download',
                                                  'AMACTION:MyTWC > Billing > Statement History > Statement Download')
                           THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- One Time Payment Start
SELECT '\n\nNow selecting One Time Payment Start...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'One Time Payment Start' as metric,
    SIZE(COLLECT_SET(case when message__name in ('MyTWC > Billing > Make Payment > Payment Confirmation') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- One Time Payment Success
SELECT '\n\nNow selecting One Time Payment Success...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'One Time Payment Success' as metric,
    SIZE(COLLECT_SET(case when message__name in ('MyTWC > Billing > Make Payment > Successful Payment') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- AutoPay Enroll Start
SELECT '\n\nNow selecting AutoPay Enroll Start...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'AutoPay Enroll Start' as metric,
    SIZE(COLLECT_SET(case when message__name in ( 'AutoPay > Setup > Payment Method > New Credit Card or Debit Card',
                                                  'AutoPay > Setup > Payment Method',
                                                  'AutoPay > Setup > Payment Method > Saved Card',
                                                  'AutoPay > Setup > Payment Method > Bank Account',
                                                  'AutoPay > Setup > Payment Method > Saved EFT',
                                                  'AutoPay > Setup > Payment Method > Credit Card',
                                                  'AutoPay > Setup > Payment Method > Debit Card')
                          THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- AutoPay Enroll Success
SELECT '\n\nNow selecting AutoPay Enroll Success...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_twc_raw PARTITION(${env:ymd})
SELECT
    'TWCC' AS company,
    'AutoPay Enroll Success' as metric,
    SIZE(COLLECT_SET(case when message__name in ( 'AutoPay > Setup > New Credit Card or Debit Card > Success',
                                                  'AutoPay > Setup > Saved Card > Success',
                                                  'AutoPay > Setup > Bank Account > Success',
                                                  'AutoPay > Setup > Saved EFT > Success',
                                                  'AutoPay > Setup > Debit Card > Success',
                                                  'AutoPay > Setup > Credit Card > Success')
                         THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_twc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;
