-- QUERY DEFINITIONS FOR sub_users_created_on_net
-- LAST UPDATED 2017-10-13 By Douglas Prince P2759846

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
 -- Metric not reported for RES


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  sum(if(state__view__current_page__page_name = 'Manage Users Page' AND visit__application_details__referrer_link = 'Add New User Confirm Info Next' and visit__isp__isp RLIKE 'charter.*',1,0)) as sub_users_created_on_net
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  sum(if(state__view__current_page__elements__name = 'my account > users > add user save' and state__view__current_page__page_name = 'my account > users' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),1,0)) as sub_users_created_on_net
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- Not Available for BHN SMB
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as sub_users_created_on_net
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
