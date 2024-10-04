-- QUERY DEFINITIONS FOR id_recovery_successes_on_net
-- LAST UPDATED 2017-10-13 By Douglas Prince P2759846

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.btm', 'Recover-final2.btm') THEN visit__visit_id ELSE NULL END))
    + SIZE(COLLECT_SET(CASE WHEN message__name IN ('Recover-final1.bam', 'Recover-final2.bam') THEN visit__visit_id ELSE NULL END)) as id_recovery_successes_on_net
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
  -- Not currently tracked for TWC RES
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  CAST(NULL AS INT) as id_recovery_successes_on_net
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- Not currently tracked for BHN
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as id_recovery_successes_on_net
FROM net_monthly_bhn_sso_metrics_manual
WHERE year_month = '${env:YEAR_MONTH}'
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-CHTR' as legacy_footprint,
  size(collect_set(if(message__name = 'Account Recovery Return to Login' and message__category = 'Custom Link' and visit__isp__isp RLIKE 'charter.*', visit__visit_id,NULL))) as id_recovery_successes_on_net
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-TWC' as legacy_footprint,
  size(collect_set(if(state__view__current_page__page_name = 'bc > forgot username> email sent' and (visit__isp__isp = 'rr.com' or visit__isp__isp = 'twcable.com' or visit__isp__isp = 'twcbiz.com'),visit__visit_id,NULL))) as id_recovery_successes_on_net
FROM twcbusglobal_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- Not currently tracked for BHN
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as id_recovery_successes_on_net
FROM sbnet_monthly_bhn_sso_metrics_manual
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
