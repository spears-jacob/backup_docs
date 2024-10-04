-- QUERY DEFINITIONS FOR view_online_statement_visitors
-- LAST UPDATED 2017-09-10 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(IF(message__name IN ('View Current Bill', 'View Statement'),visit__device__enc_uuid,NULL))) as view_online_statement_visitors
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name = 'ebpp:statement download:current' AND array_contains(message__feature__name,'Instance of eVar7'),visit__device__enc_uuid,NULL))) as view_online_statement_visitors
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  SIZE(COLLECT_SET(IF(ARRAY_CONTAINS(message__feature__name,'Custom Event 11') AND state__view__current_page__page_type='RES',visit__device__enc_uuid,NULL))) as view_online_statement_visitors
FROM bhn_bill_pay_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(IF(state__view__current_page__sub_section = 'View Statements',visit__device__enc_uuid,NULL))) as view_online_statement_visitors
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  SIZE(COLLECT_SET(IF(state__view__current_page__elements__name LIKE '%billing > statements: statement download%',visit__device__enc_uuid,NULL))) as view_online_statement_visitors
FROM twcmyacct_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  SIZE(COLLECT_SET(IF(array_contains(message__feature__name,'Custom Event 11') AND state__view__current_page__page_type='SMB',visit__device__enc_uuid,NULL))) as view_online_statement_visitors
FROM bhn_bill_pay_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
