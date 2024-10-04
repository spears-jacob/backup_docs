-- QUERY DEFINITIONS FOR total_id_recovery_successes
-- LAST UPDATED 2017-10-13 By Douglas Prince P2759846

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.btm', 'Recover-final2.btm') THEN visit__visit_id ELSE NULL END))
    + SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.bam', 'Recover-final2.bam') THEN visit__visit_id ELSE NULL END))
    + SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.nbtm', 'Recover-final2.nbtm') THEN visit__visit_id ELSE NULL END)) as total_id_recovery_successes
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SIZE(COLLECT_SET(IF(state__view__current_page__page_name = 'cla > retrieve username > username displayed' OR state__view__current_page__page_name = 'cla > retrieve username > email sent',visit__visit_id,NULL))) as total_id_recovery_successes
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- Not currently tracked for BHN
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as total_id_recovery_successes
FROM net_monthly_bhn_sso_metrics_manual
WHERE year_month = '${env:YEAR_MONTH}'
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  size(collect_set(if(message__name = 'Account Recovery Return to Login' and message__category = 'Custom Link', visit__visit_id,NULL))) as total_id_recovery_successes
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  size(collect_set(if(state__view__current_page__page_name = 'bc > forgot username> email sent',visit__visit_id,NULL))) as total_id_recovery_successes
FROM twcbusglobal_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- Not currently tracked for BHN
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as total_id_recovery_successes
FROM sbnet_monthly_bhn_sso_metrics_manual
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
