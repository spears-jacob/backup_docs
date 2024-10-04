-- QUERY DEFINITIONS FOR view_online_statement_count
-- LAST UPDATED 2017-10-13 By Douglas Prince

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SUM(IF(message__name IN ('View Current Bill', 'View Statement'),1,0)) as view_online_statement_count
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SUM(IF(state__view__current_page__elements__name = 'ebpp:statement download:current' AND array_contains(message__feature__name,'Instance of eVar7'),1,0)) as view_online_statement_count
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  SUM(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 11') AND state__view__current_page__page_type='RES',1,0)) as view_online_statement_count
FROM bhn_bill_pay_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(CASE WHEN state__view__current_page__page_name = 'Statements' THEN visit__visit_id ELSE NULL END))
      + SIZE(COLLECT_SET(CASE WHEN message__category = 'Custom Link' AND message__name = 'Download Statement' THEN visit__visit_id ELSE NULL END)) as view_online_statement_count
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  sum(if(state__view__current_page__elements__name LIKE '%billing > statements: statement download%',1,0)) as view_online_statement_count
FROM twcmyacct_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  sum(if(array_contains(message__feature__name,'Custom Event 11') AND state__view__current_page__page_type='SMB',1,0)) as view_online_statement_count
FROM bhn_bill_pay_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
