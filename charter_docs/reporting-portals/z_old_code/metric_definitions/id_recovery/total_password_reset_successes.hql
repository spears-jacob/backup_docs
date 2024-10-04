-- QUERY DEFINITIONS FOR total_password_reset_successes
-- LAST UPDATED 2017-10-13 By Douglas Prince P2759846

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.btm') THEN visit__visit_id ELSE NULL END))
    + SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.bam') THEN visit__visit_id ELSE NULL END))
    + SIZE(COLLECT_SET(CASE WHEN message__name IN ('Reset-final.nbtm') THEN visit__visit_id ELSE NULL END)) as total_password_reset_successes
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > reset password > change password confirmation',visit__visit_id,NULL))) as total_password_reset_successes
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  MAX(IF(metric = 'password_reset_successes',value,NULL)) as total_password_reset_successes
FROM net_monthly_bhn_sso_metrics_manual
WHERE year_month = '${env:YEAR_MONTH}'
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  size(collect_set(if(message__name RLIKE 'Account Recovery Password Reset Complete.*' AND message__category = 'Custom Link', visit__visit_id, NULL))) as total_password_reset_successes
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  size(collect_set(if(state__view__current_page__page_name = 'bc > forgot password > change password confirmation',visit__visit_id,NULL))) as total_password_reset_successes
FROM twcbusglobal_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- Not Tracked for BHN SMB
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as total_password_reset_successes
FROM net_monthly_bhn_sso_metrics_manual
WHERE year_month = '${env:YEAR_MONTH}'
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
