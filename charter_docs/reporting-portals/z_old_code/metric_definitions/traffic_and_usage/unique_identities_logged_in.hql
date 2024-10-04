-- QUERY DEFINITIONS FOR unique_identities_logged_in
-- LAST UPDATED 2017-08-09 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(username)) as unique_identities_logged_in
FROM federated_id
WHERE partition_date_denver = DATE_SUB(current_date,2)
  AND source_app IN ('portals-idp', 'portals-idp-twc', 'portals-idp-bhn')  -- RES
  AND response_code = 'UNIQUE_AUTH'
  AND charter_login <> ''
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SIZE(COLLECT_SET(username)) as unique_identities_logged_in
FROM federated_id
WHERE partition_date_denver = DATE_SUB(current_date,2)
  AND source_app IN ('portals-idp', 'portals-idp-twc', 'portals-idp-bhn')  -- RES
  AND response_code = 'UNIQUE_AUTH'
  AND twc_login <> ''
;

-- L-BHN Query
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  SIZE(COLLECT_SET(username)) as unique_identities_logged_in
FROM federated_id
WHERE partition_date_denver = DATE_SUB(current_date,2)
  AND source_app IN ('portals-idp', 'portals-idp-twc', 'portals-idp-bhn')  -- RES
  AND response_code = 'UNIQUE_AUTH'
  AND bh_login <> ''
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(username)) as unique_identities_logged_in
FROM federated_id
WHERE partition_date_denver = DATE_SUB(current_date,2)
  AND source_app IN ('portals-idp-comm') -- ALL SMB
  AND response_code = 'UNIQUE_AUTH'
  AND charter_login <> ''
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  SIZE(COLLECT_SET(username)) as unique_identities_logged_in
FROM federated_id
WHERE partition_date_denver = DATE_SUB(current_date,2)
  AND source_app IN ('portals-idp-comm') -- ALL SMB
  AND response_code = 'UNIQUE_AUTH'
  AND twc_login <> ''
;

-- L-BHN Query
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  SIZE(COLLECT_SET(username)) as unique_identities_logged_in
FROM federated_id
WHERE partition_date_denver = DATE_SUB(current_date,2)
  AND source_app IN ('portals-idp-comm') -- ALL SMB
  AND response_code = 'UNIQUE_AUTH'
  AND bh_login <> ''
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
