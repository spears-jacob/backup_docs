-- QUERY DEFINITIONS FOR my_account_page_views
-- LAST UPDATED 2017-08-09 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  sum(if(state__view__current_page__section = 'My Account' and message__category ='Page View',1,0)) as my_account_page_views
FROM net_events
WHERE partition_date >= DATE_SUB(current_date,1)
;
-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SUM(IF(message__category = 'Page View' and state__view__current_page__section = 'services',1,0)) as my_account_page_views
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'RES' as portal_platform,
  'L-BNN' as legacy_footprint,
  SUM(IF(message__category = 'Page View' and state__view__current_page__section = 'my services active',1,0)) as my_account_page_views
FROM bhn_residential_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  sum(if(message__category = 'Page View',1,0)) 
    - sum(if(message__category = 'Page View' AND state__view__current_page__page_name = 'Login' AND state__view__current_page__section = 'Login' ,1,0)) as my_account_page_views
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  sum(if(message__category = 'Page View',1,0)) 
    - sum(if(message__category = 'Page View' AND state__view__current_page__page_name = 'my account > login',1,0)) as my_account_page_views
FROM twcmyacct_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  SUM(IF(message__category='Page View' 
    AND lower(state__view__current_page__page_id) NOT LIKE '%bhnbusiness%'
    AND lower(state__view__current_page__page_id) NOT LIKE '%login%'
    AND lower(state__view__current_page__page_id) NOT LIKE '%businesssolutions.brighthouse.com/home%'
    AND lower(state__view__current_page__page_id) NOT LIKE '%businesssolutions.brighthouse.com/content/mobile/business/home%',1,0)) as my_account_page_views
FROM bhnmyservices_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
