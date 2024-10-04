-- QUERY DEFINITIONS FOR login_attempts
-- LAST UPDATED 2017-09-08 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT 
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SUM(if(check_level = 'Summary' AND source_app IN ('portals-idp'),total,0)) as login_attempts
FROM federated_id_auth_attempts_total
WHERE partition_date_denver = DATE_SUB(current_date,2)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SUM(if(check_level = 'Summary' AND source_app IN ('portals-idp-twc'),total,0)) as login_attempts
FROM federated_id_auth_attempts_total
WHERE partition_date_denver = DATE_SUB(current_date,2)
;

-- L-BHN Query
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  SUM(if(check_level = 'Summary' AND source_app IN ('portals-idp-bhn'),total,0)) as login_attempts
FROM federated_id_auth_attempts_total
WHERE partition_date_denver = DATE_SUB(current_date,2)
;


-- ***** SMB PORTAL QUERIES *****
-- Attempts can only be calculated for All companies combined for SMB using Fed ID ... will give the alias "L-CHTR" to make JOIN easier
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SUM(if((check_level = 'Success' OR check_level = 'Failure') AND source_app IN ('portals-idp-comm'),total,0)) as login_attempts
FROM federated_id_auth_attempts_total
WHERE partition_date_denver = DATE_SUB(current_date,2)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
