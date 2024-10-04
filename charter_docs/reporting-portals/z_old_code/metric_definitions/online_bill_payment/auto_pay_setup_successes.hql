-- QUERY DEFINITIONS FOR auto_pay_setup_successes
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
    "AutoPay-noBalance-Credit-Confirm",
    "AutoPay-noBalance-Checking-Confirm")
    OR (state__view__current_page__name = "AutoPay-noBalance-Savings-Confirm" AND state__view__previous_page__name = "AutoPay-noBalance-Savings-Review")
    OR (state__view__current_page__name = "AutoPay-wBalance-Credit-Confirm" AND state__view__previous_page__name = "AutoPay-wBalance-Credit-Review")
    OR (state__view__current_page__name = "AutoPay-wBalance-Checking-Confirm" AND state__view__previous_page__name = "AutoPay-wBalance-Checking-Review")
    OR (state__view__current_page__name = "AutoPay-wBalance-Savings-Review" AND state__view__previous_page__name = "AutoPay-wBalance-Savings-Confirm"),
    visit__visit_id,NULL))) as auto_pay_setup_successes
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

  --MONTHLY (AGG Table Query)
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SUM(IF(step IN ('6a','6b','6c','9a','9b','9c'),count_visits,0)) as auto_pay_setup_successes
FROM net_bill_pay_analytics_monthly
WHERE year_month = DATE_YEARMONTH(ADD_MONTHS(current_date,-1))
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  COUNT(DISTINCT (CASE WHEN state__view__current_page__page_name = 'ebpp > bill pay > step 4 / thank you' 
    AND operation__operation_type 
    IN ('recurring:unknown > cc','recurring:unknown > ach','recurring:unknown > dc') 
    THEN visit__visit_id END)) as auto_pay_setup_successes
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  COUNT(DISTINCT (CASE WHEN(array_contains(message__feature__name, 'Custom Event 24') AND state__view__current_page__page_type='RES') THEN visit__visit_id END)) as auto_pay_setup_successes
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
  size(collect_set(if(state__view__current_page__page_name = 'Confirm  Auto Pay Setup' and state__view__previous_page__page_name = 'Verify Auto Pay Setup' and visit__application_details__referrer_link = 'Save Auto Pay',visit__visit_id,NULL))) as auto_pay_setup_successes
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  size(collect_set(if(state__view__current_page__elements__name like '%step 4%' and state__view__current_page__elements__name like '%recurring%',visit__visit_id,NULL))) as auto_pay_setup_successes
FROM twcmyacct_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  size(collect_set(if(array_contains(message__feature__name,'Custom Event 24') AND state__view__current_page__page_type='SMB',visit__visit_id,NULL))) AS auto_pay_setup_successes
FROM bhn_bill_pay_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
