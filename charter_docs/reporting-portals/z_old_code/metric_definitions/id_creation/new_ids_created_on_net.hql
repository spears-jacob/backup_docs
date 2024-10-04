-- QUERY DEFINITIONS FOR total_new_ids_created_on_net
-- LAST UPDATED 2017-08-11 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(IF message__name IN (
  'my-account.create-id-final.bam', 
  'my-account.create-id-final.btm'),visit__visit_id,NULL)) as total_new_ids_created_on_net
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SUM(IF(array_contains(message__feature__name,'Custom Event 5') and visit__connection__network_status='cla 3.0:in home',1,0)) as total_new_ids_created_on_net
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- Not Available
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as total_new_ids_created_on_net
FROM net_monthly_bhn_accounts_manual
WHERE year_month = '${env:YEAR_MONTH}'
;



-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  size(collect_set(if(message__name = 'Create Account Summary' and visit__isp__isp LIKE 'charter%',visit__visit_id,NULL))) as total_new_ids_created_on_net
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  size(collect_set(if(array_contains(message__name,'CLA: Create Your Account') and state__view__current_page__page_id like 'https://registration.timewarnercable.com/businessclass%' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),visit__visit_id,NULL))) as total_new_ids_created_on_net
FROM twcbusglobal_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- Not Available
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as total_new_ids_created_on_net
FROM sbnet_exec_monthly_bhn_accounts_manual
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
