USE ${env:ENVIRONMENT};

-- Queries started failing due to java.lang.OutOfMemoryError: Java heap space
set hive.auto.convert.join=false;

-- In the CASE WHEN statement regarding company, UNAUTH refers to actions
-- taken prior to authentication, i.e. 'Unauthenticated'.
-- Legacy companies are indistinguishable until authentication occurs.

-- Insert OVERWRITE on first metric clears out partition for subsequent inserts

SELECT '\n\nNow running asp_app_agg_02d_my_spc...\n\n';

-- Page Views (uses insert overwrite to clear partition)
SELECT '\n\nNow selecting Page Views...\n\n';

INSERT OVERWRITE TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Page Views' as metric,
    SIZE(COLLECT_LIST(case when message__category in ('Page View') THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap} as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section;


-- Visits
SELECT '\n\nNow selecting Visits...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Visits' as metric,
    SIZE(COLLECT_SET(visit__visit_id)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND state__view__previous_page__sub_section IN('BH','CHARTER','TWC') -- 2018-01-16 Authenticated Only
GROUP BY ${env:ap}, state__view__previous_page__sub_section ;

-- Unique Visitors
SELECT '\n\nNow selecting Unique Visitors...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Unique Visitors' as metric,
    SIZE(COLLECT_SET(visit__device__enc_uuid)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND state__view__previous_page__sub_section IN('BH','CHARTER','TWC') -- 2018-01-16 Authenticated Only
GROUP BY ${env:ap}, state__view__previous_page__sub_section ;


-- Cancelled Appointments
SELECT '\n\nNow selecting Cancelled Appointments...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Cancelled Appointments' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:CancelAppointment Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND state__view__previous_page__sub_section IN('BH','CHARTER','TWC')
GROUP BY ${env:ap}, state__view__previous_page__sub_section
;

-- Rescheduled Appointments
SELECT '\n\nNow selecting Rescheduled Appointments...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Rescheduled Appointments' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:RescheduleAppointment Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section
;

-- Refresh Digital Receiver Requests
SELECT '\n\nNow selecting Refresh Digital Receiver Requests...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Refresh Digital Receiver Requests' as metric,
    SIZE(COLLECT_LIST(case when message__name in ('AMACTION:RefreshEquipment Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section
HAVING SIZE(COLLECT_LIST(case when message__name in ('AMACTION:RefreshEquipment Svc Call Success') THEN visit__visit_id ELSE NULL END)) > 0
;


-- Modem Router Resets
SELECT '\n\nNow selecting Modem Router Resets...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Modem Router Resets' as metric,
    SIZE(COLLECT_LIST(case when message__name IN ('AMACTION:RebootEquipment Svc Call Success', 'AMACTION:Equipment Internet reboot Modem success', 'AMACTION:Equipment Internet reboot Router success') THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND state__view__previous_page__sub_section IN('BH','CHARTER','TWC')
AND message__category = 'Custom Link'
GROUP BY ${env:ap}, state__view__previous_page__sub_section
HAVING SIZE(COLLECT_LIST(case when message__name in ('AMACTION:RebootEquipment Svc Call Success', 'AMACTION:Equipment Internet reboot Modem success', 'AMACTION:Equipment Internet reboot Router success', 'Equipment Internet Reset Success View','AMACTION:Troubleshoot ProblemSolved Trigger','Equipment Voice Reset Success View','Equipment TV Reset Success View') THEN visit__visit_id ELSE NULL END)) > 0
;


-- Support Page Views
-- 2017-10-03 adding message__name=AMACTION:Moving Trigger are temporarily added
-- to Support Page Views


SELECT '\n\nNow selecting Support Page Views...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Support Page Views' as metric,
    SIZE(COLLECT_LIST(case when message__name in ('Support View',
                                                 'Passpoint Setup View',
                                                 'Tutorial Walkthrough First Use View',
                                                 'Channel Lineups',
                                                 'Moving Form NewAddress View',
                                                 'Program Your Remote',
                                                 'Locations Map',
                                                 'Terms And Conditions',
                                                 'AMACTION:Moving Trigger') THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
       WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
       WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
       WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
       ELSE 'UNDEFINED'
    END IN ('BHN','CHTR','TWCC')
GROUP BY ${env:ap}, state__view__previous_page__sub_section ;


-- Call Support
SELECT '\n\nNow selecting Call Support...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Call Support' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:Call Support Trigger') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section ;


-- View Statement
SELECT '\n\nNow selecting View Statement...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'View Statement' as metric,
    SIZE(COLLECT_LIST(case when message__name in
                ('AMACTION:PDFStatementDownload Svc Call Success',
                 'AMACTION:PDF Single Statement Svc Call Success')
                      THEN visit__visit_id ELSE NULL END)) as value,
    'instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND state__view__previous_page__sub_section IN('BH','CHARTER','TWC')
GROUP BY ${env:ap}, state__view__previous_page__sub_section ;


-- One Time Payment Start
SELECT '\n\nNow selecting One Time Payment Start...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'One Time Payment Start' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:Dashboard Make Payment Button Trigger',
                                                 'AMACTION:Make Payment From BillPayTab',
                                                 'AMACTION:Payment View Make Payment Trigger') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND state__view__previous_page__sub_section IN('BH','CHARTER','TWC')
GROUP BY ${env:ap}, state__view__previous_page__sub_section ;


-- One Time Payment Success
SELECT '\n\nNow selecting One Time Payment Success...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'One Time Payment Success' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:MakeOneTimePayment Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND state__view__previous_page__sub_section IN('BH','CHARTER','TWC')
GROUP BY ${env:ap}, state__view__previous_page__sub_section ;


-- AutoPay Enroll Start
SELECT '\n\nNow selecting AutoPay Enroll Start...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'AutoPay Enroll Start' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:Payment View Auto Pay Enabled Trigger') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND state__view__previous_page__sub_section IN('BH','CHARTER','TWC')
GROUP BY ${env:ap}, state__view__previous_page__sub_section ;


-- AutoPay Enroll Success
SELECT '\n\nNow selecting AutoPay Enroll Success...\n\n';


INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'AutoPay Enroll Success' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:AddAutoPay Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND state__view__previous_page__sub_section IN('BH','CHARTER','TWC')
GROUP BY ${env:ap}, state__view__previous_page__sub_section ;


-- Forgot Password Attempt
-- DPrince 2018-05-31 update
SELECT '\n\nNow selecting Forgot Password Attempt...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Forgot Password Attempt' as metric,
    SIZE(COLLECT_SET(case when message__name in ('FP Username View','AMACTION:Forgot Password') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section
HAVING SIZE(COLLECT_SET(case when message__name in ('FP Username View') THEN visit__visit_id ELSE NULL END)) > 0
;


-- Forgot Password Success
-- 2017-10-03: This will be BHN only for the time being
SELECT '\n\nNow selecting Forgot Password Success...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Forgot Password Success' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:ForgotPasswordStep3 Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section
HAVING SIZE(COLLECT_SET(case when message__name in ('AMACTION:ForgotPasswordStep3 Svc Call Success') THEN visit__visit_id ELSE NULL END)) > 0;


-- Forgot Username Attempt
-- DPrince 2018-05-31 update
SELECT '\n\nNow selecting Forgot Username Attempt...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Forgot Username Attempt' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:Forgot Username') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section
HAVING SIZE(COLLECT_SET(case when message__name in ('Forgot Username View') THEN visit__visit_id ELSE NULL END)) > 0
;


-- Forgot Username Success
-- 2017-10-03: This will be BHN only for the time being
SELECT '\n\nNow selecting Forgot Username Success...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Forgot Username Success' as metric,
    SIZE(COLLECT_SET(case when message__name in ('AMACTION:ForgotUsername Svc Call Success') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section
HAVING SIZE(COLLECT_SET(case when message__name in ('AMACTION:ForgotUsername Svc Call Success') THEN visit__visit_id ELSE NULL END)) > 0
;

---------------------------------------------------------
-- additions for Daily Dashboard
---------------------------------------------------------
-- Visits to the main sections of the app via Omniture

-- Dashboard View
SELECT '\n\nNow selecting Dashboard View visits...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Dashboard View' as metric,
    SIZE(COLLECT_SET(case when message__name in ('Dashboard View') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section
HAVING SIZE(COLLECT_SET(case when message__name in ('Dashboard View') THEN visit__visit_id ELSE NULL END)) > 0
;

-- Equipment List View
SELECT '\n\nNow selecting Equipment List View Visits...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Equipment List View' as metric,
    SIZE(COLLECT_SET(case when message__name in ('Equipment List View') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section
HAVING SIZE(COLLECT_SET(case when message__name in ('Equipment List View') THEN visit__visit_id ELSE NULL END)) > 0
;

-- Account View
SELECT '\n\nNow selecting Account View Visits...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Account View' as metric,
    SIZE(COLLECT_SET(case when message__name in ('Account View') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section
HAVING SIZE(COLLECT_SET(case when message__name in ('Account View') THEN visit__visit_id ELSE NULL END)) > 0
;

-- Bill Pay View
SELECT '\n\nNow selecting Bill Pay View Visits...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Bill Pay View' as metric,
    SIZE(COLLECT_SET(case when message__name in ('Bill Pay View') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section
HAVING SIZE(COLLECT_SET(case when message__name in ('Bill Pay View') THEN visit__visit_id ELSE NULL END)) > 0
;

-- Support View
SELECT '\n\nNow selecting Support View Visits...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Support View' as metric,
    SIZE(COLLECT_SET(case when message__name in ('Support View') THEN visit__visit_id ELSE NULL END)) as value,
    'visits' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
GROUP BY ${env:ap}, state__view__previous_page__sub_section
HAVING SIZE(COLLECT_SET(case when message__name in ('Support View') THEN visit__visit_id ELSE NULL END)) > 0
;

-- Crashes
-- When post_event_list contains 200, That means that 'Custom Event 1' has occured.
-- (visit__application_details__referrer_link) contains
--
-- custom event1 = crash   = post_event_list:200
-- custom event2 = launch  = post_event_list:201
-- custom event3 = install = post_event_list:202
-- custom event4 = upgrade = post_event_list:203
SELECT '\n\nNow selecting Crashes...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Crashes' as metric,
    count(1) as value,
    'Instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND visit__application_details__referrer_link like '%200%'
GROUP BY ${env:ap}, state__view__previous_page__sub_section
;

SELECT '\n\nNow selecting Crashes by platform (iOS or Android)...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    CASE WHEN visit__application_details__app_version RLIKE 'iOS.*' THEN 'Crashes iOS'
         WHEN visit__application_details__app_version RLIKE 'Android.*' THEN 'Crashes Android'
         ELSE 'Crashes UNDEFINED'
    END as metric,
    count(1) as value,
    'Instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND visit__application_details__referrer_link like '%200%'
GROUP BY ${env:ap}, state__view__previous_page__sub_section, visit__application_details__app_version
;

SELECT '\n\nNow selecting Launches...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    'Launches' as metric,
    count(1) as value,
    'Instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND visit__application_details__referrer_link like '%201%'
GROUP BY ${env:ap}, state__view__previous_page__sub_section
;

SELECT '\n\nNow selecting Launches by platform (iOS or Android)...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_agg_my_spc_raw PARTITION(${env:ymd})
SELECT
    CASE WHEN state__view__previous_page__sub_section = 'BH' THEN 'BHN'
         WHEN state__view__previous_page__sub_section = 'CHARTER' THEN 'CHTR'
         WHEN state__view__previous_page__sub_section = 'TWC' THEN 'TWCC'
         WHEN state__view__previous_page__sub_section IS NULL THEN 'UNAUTH'
         ELSE 'UNDEFINED'
    END as company,
    CASE WHEN visit__application_details__app_version RLIKE 'iOS.*' THEN 'Launches iOS'
         WHEN visit__application_details__app_version RLIKE 'Android.*' THEN 'Launches Android'
         ELSE 'Launches UNDEFINED'
    END as metric,
    count(1) as value,
    'Instances' as unit,
    ${env:ap}  as ${env:ymd}
FROM asp_v_spc_app_events
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
AND visit__application_details__referrer_link like '%201%'
GROUP BY ${env:ap}, state__view__previous_page__sub_section, visit__application_details__app_version
;
