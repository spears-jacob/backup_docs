-- QUERY DEFINITIONS FOR ask_charter_requests
-- LAST UPDATED 2017-08-09 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  sum(if(message__name IN ('Ask-Spectrum', 'Ask Spectrum'),1,0)) as ask_charter_requests
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SIZE(COLLECT_SET(IF(LOWER(state__search__text) like '%askamy%',visit__visit_id,NULL))) as ask_charter_requests
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- N/A, ask charter requests not captured by BHN SMB Adobe
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as ask_charter_requests
FROM bhn_residential_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** SMB PORTAL QUERIES *****
  -- N/A, ask charter requests not captured by SMB Adobe


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
