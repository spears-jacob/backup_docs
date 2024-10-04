USE ${env:ENVIRONMENT};

-- Queries started failing due to java.lang.OutOfMemoryError: Java heap space
set hive.auto.convert.join=false;

-- Insert OVERWRITE on first metric clears out partition for subsequent inserts

SELECT '\n\nNow running asp_app_agg_02b_my_bhn...\n\n';

-- Page Views (uses insert overwrite to clear partition)
SELECT '\n\nNow selecting Page Views...\n\n';

INSERT OVERWRITE TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Page Views' as metric,
    SIZE(COLLECT_LIST(case when message__category in ('Page View') THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap} as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap};


-- Visits
SELECT '\n\nNow selecting Visits...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Visits' as metric,
    SIZE(COLLECT_SET(visit__visit_id)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Unique Visitors
SELECT '\n\nNow selecting Unique Visitors...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Unique Visitors' as metric,
    SIZE(COLLECT_SET(visit__device__enc_uuid)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Cancelled Appointments
SELECT '\n\nNow selecting Cancelled Appointments...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Cancelled Appointments' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:CancelAppointment Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Rescheduled Appointments
SELECT '\n\nNow selecting Rescheduled Appointments...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Rescheduled Appointments' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:RescheduleAppointment Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Refresh Digital Receiver Requests
SELECT '\n\nNow selecting Refresh Digital Receiver Requests...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Refresh Digital Receiver Requests' as metric,
    SIZE(COLLECT_LIST(case when message__name in ('AMACTION:RefreshEquipment Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Modem Router Resets
SELECT '\n\nNow selecting Modem Router Resets...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Modem Router Resets' as metric,
    SIZE(COLLECT_LIST(case when message__name in ('AMACTION:RebootEquipment Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Support Page Views
SELECT '\n\nNow selecting Support Page Views...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Support Page Views' as metric,
    SIZE(COLLECT_LIST(case when message__name in ('Support View',
                                                 'Passpoint Setup View',
                                                 'Tutorial Walkthrough First Use View',
                                                 'Channel Lineups',
                                                 'Moving Form NewAddress View',
                                                 'Program Your Remote',
                                                 'Locations Map',
                                                 'Terms And Conditions') THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Call Support
SELECT '\n\nNow selecting Call Support...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Call Support' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:Call Support Trigger') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- View Statement
SELECT '\n\nNow selecting View Statement...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'View Statement' as metric,
    SIZE(COLLECT_LIST(case when message__name in ('AMACTION:PDFStatementDownload Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- One Time Payment Start
SELECT '\n\nNow selecting One Time Payment Start...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'One Time Payment Start' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:Dashboard Make Payment Button Trigger',
                                                 'AMACTION:Make Payment From BillPayTab') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- One Time Payment Success
SELECT '\n\nNow selecting One Time Payment Success...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'One Time Payment Success' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:Paid with an existing Payment',
                                                 'AMACTION:Paid with new Payment Method') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- AutoPay Enroll Start
SELECT '\n\nNow selecting AutoPay Enroll Start...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'AutoPay Enroll Start' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:Payment View Auto Pay Enabled Trigger') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- AutoPay Enroll Success
SELECT '\n\nNow selecting AutoPay Enroll Success...\n\n';


INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'AutoPay Enroll Success' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:AddAutoPay Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Forgot Password Attempt
SELECT '\n\nNow selecting Forgot Password Attempt...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Forgot Password Attempt' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:Forgot Password') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Forgot Password Success
SELECT '\n\nNow selecting Forgot Password Success...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Forgot Password Success' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:ForgotPasswordStep3 Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Forgot Username Attempt
SELECT '\n\nNow selecting Forgot Username Attempt...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Forgot Username Attempt' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:Forgot Username') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;

-- Forgot Username Success
SELECT '\n\nNow selecting Forgot Username Success...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_bhn_raw PARTITION(${env:ymd})
SELECT
    'BHN' as company,
    'Forgot Username Success' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:ForgotUsername Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_bhn_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap} ;
