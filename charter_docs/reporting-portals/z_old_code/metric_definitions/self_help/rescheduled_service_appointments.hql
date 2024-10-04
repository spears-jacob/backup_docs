-- QUERY DEFINITIONS FOR rescheduled_service_appointments
-- LAST UPDATED 2017-08-09 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'RES' as portal_platform,
  'L-CHTR' as legacy_footprint,
  SIZE(COLLECT_SET(IF(message__name IN ('reschedule-confirm'), visit__visit_id, NULL))) as recheduled_service_appoint_count
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'RES' as portal_platform,
  'L-TWC' as legacy_footprint,
  SIZE(COLLECT_SET(IF(state__view__current_page__sub_section LIKE '%reschedule%',visit__visit_id,NULL))) as recheduled_service_appoint_count
FROM twc_residential_global_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-BHN Query
  -- N/A, service appointments not captured by BHN SMB Adobe
SELECT
  'RES' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as recheduled_service_appoint_count
FROM bhn_residential_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  size(collect_set(if(message__name = 'Confirm Reschedule Appointment' and state__view__current_page__section = 'Appointment Tracking',visit__visit_id,NULL))) as recheduled_service_appoint_count
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- L-TWC Query
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  size(collect_set(if(state__view__current_page__elements__name = 'my account > support > reschedule appointment submit',visit__visit_id,NULL))) as recheduled_service_appoint_count
FROM sbnet_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;


-- L-BHN Query
  -- N/A, service appointments not captured by BHN SMB Adobe
SELECT
  'SMB' as portal_platform,
  'L-BHN' as legacy_footprint,
  CAST(NULL AS INT) as recheduled_service_appoint_count as recheduled_service_appoint_count
FROM bhnmyservices_events
WHERE partition_date_utc >= DATE_SUB(current_date,2)
  AND epoch_converter(message__timestamp*1000, 'America/Denver') = DATE_SUB(current_date,1)
;

-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
