-- QUERY DEFINITIONS FOR auto_pay_setup_attempts
-- LAST UPDATED 2017-09-10 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
  --DAILY (Events Query)
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(IF(state__view__current_page__name IN(
    'AutoPay-wBalance-Credit-Review',
    'AutoPay-wBalance-Checking-Review',
    'AutoPay-wBalance-Savings-Review',
    'AutoPay-noBalance-Credit-Review',
    'AutoPay-noBalance-Checking-Review',
    'AutoPay-noBalance-Savings-Review'),visit__visit_id,NULL))) as auto_pay_setup_attempts
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

  --MONTHLY (AGG Table Query)
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SUM(IF(step IN ('5a','5b','5c','8a','8b','8c'),count_visits,0)) as auto_pay_setup_attempts
FROM net_bill_pay_analytics_monthly
WHERE year_month = DATE_YEARMONTH(ADD_MONTHS(current_date,-1))
;

-- L-TWC Query
  -- N/A, payment attempts not captured accurately by TWC Adobe
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  CAST(NULL AS INT) as auto_pay_setup_attempts
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  COUNT(DISTINCT (CASE WHEN(array_contains(message__feature__name, 'Custom Event 19') AND state__view__current_page__page_type='RES') THEN visit__visit_id END)) as auto_pay_setup_attempts
FROM bhn_bill_pay_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  size(collect_set(if(message__name = 'Save Auto-Pay' and state__view__current_page__sub_section = 'Verify Auto-Pay',visit__visit_id,NULL))) as auto_pay_setup_attempts
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
  -- N/A, payment attempts not captured accurately by TWC Adobe
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  CAST(NULL AS INT) as auto_pay_setup_attempts
FROM twcmyacct_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  size(collect_set(if(array_contains(message__feature__name,'Custom Event 19') AND state__view__current_page__page_type='SMB',visit__visit_id,NULL))) AS auto_pay_setup_attempts
FROM bhn_bill_pay_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
