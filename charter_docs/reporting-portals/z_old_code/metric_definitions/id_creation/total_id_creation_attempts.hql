-- QUERY DEFINITIONS FOR total_id_creation_attempts
-- LAST UPDATED 2017-08-11 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  (SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.bam') THEN visit__visit_id ELSE NULL END))
      - SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.bam') AND lower(message__name) RLIKE ('.*sign\-in\-now.*') THEN visit__visit_id ELSE NULL END)))
    + (SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.btm') THEN visit__visit_id ELSE NULL END))
      - SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.btm') AND lower(message__name) RLIKE ('.*sign\-in\-now.*') THEN visit__visit_id ELSE NULL END)))
    + (SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-1.nbtm') THEN visit__visit_id ELSE NULL END))
      - SIZE(COLLECT_SET(CASE WHEN state__view__current_page__name IN ('my-account.create-id-2.nbtm') AND lower(message__name) RLIKE ('.*sign\-in\-now.*') THEN visit__visit_id ELSE NULL END)))
    as total_id_creation_attempts
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SUM(IF(array_contains(message__feature__name,'Custom Event 1'),1,0)) as total_id_creation_attempts
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- Not available
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as total_id_creation_attempts
FROM net_monthly_bhn_accounts_manual
WHERE year_month = '${env:YEAR_MONTH}'
;



-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  size(collect_set(if(message__name = 'Create Account Register',visit__visit_id,NULL))) as total_id_creation_attempts
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  size(collect_set(if(array_contains(message__name,'CLA: Create Your Account') and state__view__current_page__page_id like 'https://registration.timewarnercable.com/businessclass%',visit__visit_id,NULL))) as total_id_creation_attempts
FROM twcbusglobal_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- Not available
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as total_id_creation_attempts
FROM sbnet_exec_monthly_bhn_accounts_manual
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
