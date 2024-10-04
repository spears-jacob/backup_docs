-- QUERY DEFINITIONS FOR total_new_ids_created
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
  'my-account.create-id-final.btm', 
  'my-account.create-id-final.nbtm'),visit__visit_id,NULL)) as total_new_ids_created
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SUM(IF(array_contains(message__feature__name,'Custom Event 5'),1,0)) as bigint),NULL)) as total_new_ids_created
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- Pre-aggregated to monthly level
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  MAX(IF(site_id='003' and role='Administrator',CAST(REGEXP_REPLACE(REGEXP_REPLACE(new_accounts,'"',''),',','') as total_new_ids_created
FROM net_monthly_bhn_accounts_manual
WHERE year_month = '${env:YEAR_MONTH}'
;



-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  size(collect_set(if(message__name = 'Create Account Summary',visit__visit_id,NULL))) as total_new_ids_created
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  size(collect_set(if(array_contains(message__name,'CLA: Confirmation') and state__view__current_page__page_id like 'https://registration.timewarnercable.com/businessclass%',visit__visit_id,NULL))) as total_new_ids_created
FROM twcbusglobal_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  IF(site_id='005' and role='Administrator',CAST(REGEXP_REPLACE(REGEXP_REPLACE(new_accounts,'"',''),',','') as bigint),NULL) as total_new_ids_created
FROM sbnet_exec_monthly_bhn_accounts_manual
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
