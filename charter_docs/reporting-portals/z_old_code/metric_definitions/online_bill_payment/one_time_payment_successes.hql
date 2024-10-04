-- QUERY DEFINITIONS FOR one_time_payment_successes
-- LAST UPDATED 2017-09-10 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
  --DAILY (Events Query)
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(IF(state__view__current_page__name IN (
    'OneTime-noAutoPay-Credit-Confirm',
    'OneTime-noAutoPay-Checking-Confirm',
    'OneTime-noAutoPay-Savings-Confirm') 
    OR (state__view__current_page__name = 'OneTime-wAutoPay-Credit-Confirm' AND state__view__previous_page__name = 'OneTime-wAutoPay-Credit-Review')
    OR (state__view__current_page__name = 'OneTime-wAutoPay-Checking-Confirm' AND state__view__previous_page__name = 'OneTime-wAutoPay-Checking-Review')
    OR (state__view__current_page__name = 'OneTime-wAutoPay-Savings-Confirm' AND state__view__previous_page__name = 'OneTime-wAutoPay-Savings-Review'),visit__visit_id,NULL))) as one_time_payment_successes
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

  --MONTHLY (AGG Table Query)
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SUM(IF(step IN ('3a','3b','3c','3d','3e','3f'),count_visits,0)) as one_time_payment_successes
FROM net_bill_pay_analytics_monthly
WHERE year_month = DATE_YEARMONTH(ADD_MONTHS(current_date,-1))
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SIZE(COLLECT_SET(CASE WHEN state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you' 
    AND operation__operation_type 
    IN ('one time payment > dc',
    'one time:saved > cc',
    'one time:saved > dc',
    'one time:unknown > ach',
    'one time:unknown > cc',
    'one time:unknown > dc') 
    THEN visit__visit_id END)) as one_time_payment_successes
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  SIZE(COLLECT_SET(IF(array_contains(message__feature__name, 'Custom Event 31') AND state__view__current_page__page_type='RES',visit__visit_id,NULL))) as one_time_payment_successes
FROM bhn_bill_pay_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
-- NEED TO CHANGE
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  size(collect_set(if(state__view__current_page__page_name = 'Confirm  Payment' and state__view__previous_page__page_name = 'Verify One Time Payment' and visit__application_details__referrer_link = 'Complete Payment',visit__visit_id,NULL))) as one_time_payment_successes
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  size(collect_set(if(state__view__current_page__elements__name like '%step 4%' and (state__view__current_page__elements__name like '%fdp%' or state__view__current_page__elements__name like '%one time%'),visit__visit_id,NULL))) as one_time_payment_successes
FROM twcmyacct_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  size(collect_set(if(array_contains(message__feature__name,'Custom Event 31') AND state__view__current_page__page_type='SMB',visit__visit_id,NULL))) AS one_time_payment_successes
FROM bhn_bill_pay_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
