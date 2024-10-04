-- QUERY DEFINITIONS FOR login_successes
-- LAST UPDATED 2017-08-09 BY Zach Jesberger

USE {env:ENVIRONMENT}; 

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SUM(IF(check_level = 'Summary' AND check_result = 'Success' AND source_app IN ('portals-idp'),total,0)) as login_successes
FROM federated_id_auth_attempts_total
WHERE partition_date_denver = DATE_SUB(current_date,2)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SUM(IF(check_level = 'Summary' AND check_result = 'Success' AND source_app IN ('portals-idp-twc'),total,0)) as login_successes
FROM federated_id_auth_attempts_total
WHERE partition_date_denver = DATE_SUB(current_date,2)
;

-- L-BHN Query
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  SUM(IF(check_level = 'Summary' AND check_result = 'Success' AND source_app IN ('portals-idp-bhn'),total,0)) as login_successes
FROM federated_id_auth_attempts_total
WHERE partition_date_denver = DATE_SUB(current_date,2)
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SUM(IF(check_level = 'Success' AND source_app IN ('portals-idp-comm') AND footprint = 'Charter',total,0)) as login_successes
FROM federated_id_auth_attempts_total
WHERE partition_date_denver = DATE_SUB(current_date,2)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  SUM(IF(check_level = 'Success' AND source_app IN ('portals-idp-comm') AND footprint = 'TWC',total,0)) as login_successes
FROM federated_id_auth_attempts_total
WHERE partition_date_denver = DATE_SUB(current_date,2)
;

-- L-BHN Query
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  SUM(IF(check_level = 'Success' AND source_app IN ('portals-idp-comm') AND footprint = 'BHN',total,0)) as login_successes
FROM federated_id_auth_attempts_total
WHERE partition_date_denver = DATE_SUB(current_date,2)
;


-- ***** APP QUERIES *****
-- L-CHTR Query
SELECT
  'APP' as portal_platform,
  'L-CHTR' as legacy_footprint,
  [insert_metric_calculation] as [insert_metric_name]
FROM [insert_table_name]
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query


-- L-BHN Query
