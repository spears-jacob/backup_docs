-- QUERY DEFINITIONS FOR unique_hhs_logged_in
-- LAST UPDATED 2017-08-09 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(visit__account__enc_account_number)) as unique_hhs_logged_in
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SIZE(COLLECT_SET(visit__account__enc_account_number)) as unique_hhs_logged_in
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- N/A, account number not captured by BHN Adobe
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as unique_hhs_logged_in
FROM bhn_residential_events
;

-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(visit__account__enc_account_number)) as unique_hhs_logged_in
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  SIZE(COLLECT_SET(visit__account__enc_account_number)) as unique_hhs_logged_in
FROM twcmyacct_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- N/A, account number not captured by BHN Adobe
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as unique_hhs_logged_in
FROM bhn_residential_events
;

-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
