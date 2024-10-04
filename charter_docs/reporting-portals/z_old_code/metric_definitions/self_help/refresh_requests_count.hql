-- QUERY DEFINITIONS FOR refresh_requests_count (Refresh Digital Receivers Requests)
-- LAST UPDATED 2017-08-10 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  sum(if(message__name IN ('Refresh'),1,0)) as refresh_requests_count
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(IF(state__view__current_page__sub_section like '%reauthorize%',visit__visit_id,NULL))) 
    + SIZE(COLLECT_SET(IF(state__view__current_page__sub_section like '%reboot%',visit__visit_id,NULL))) as refresh_requests_count
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- N/A, Refresh Digital Receivers Requests not captured by BHN SMB Adobe
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as refresh_requests_count
FROM bhn_residential_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** SMB PORTAL QUERIES *****
  -- N/A, Refresh Digital Receivers Requests not captured by SMB Adobe



-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
