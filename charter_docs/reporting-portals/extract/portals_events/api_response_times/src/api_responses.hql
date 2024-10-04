USE ${env:ENVIRONMENT};

SELECT '***** inserting data into asp_api_responses_raw table ******'
;
INSERT OVERWRITE TABLE asp_api_responses_raw PARTITION (date_denver)


SELECT LOWER(visit__application_details__application_name) AS application_name,
  visit__application_details__app_version AS app_version,
  -- mso logic
  CASE
    WHEN visit__account__details__mso IN ('CHARTER', '"CHTR"') THEN 'L-CHTR'
    WHEN visit__account__details__mso IN ('BH', '"BHN"') THEN 'L-BHN'
    WHEN visit__account__details__mso IN ('TWC','"TWC"')  THEN 'L-TWC'
  ELSE visit__account__details__mso END AS mso,
  -- if api_category is missing use message category
  nvl(application__api__api_category, message__category) AS api_category,
  IF(SUBSTR(application__api__api_name,1,4) = 'http', 'BAD:httpDVR', application__api__api_name) AS api_name,
  application__api__response_code AS api_code,
  state__view__current_page__page_name AS page_name,
  COALESCE(application__api__response_time_ms,0) AS response_ms,
  ROUND(COALESCE(application__api__response_time_ms,0)/1000,1) as response_s_tenths,
  application__api__service_result AS service_result,
  epoch_converter(received__timestamp,'America/Denver') as date_denver
FROM venona_events_portals
-- look at api calls for specnet & smb
WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
  AND partition_date_hour_utc < '${env:END_DATE_TZ}'
  AND lower(visit__application_details__application_name) in ('specnet', 'smb', 'myspectrum')
  AND visit__account__details__mso != ''
  AND ISNOTNULL(visit__account__details__mso)
  AND message__name = 'apiCall'
  -- do not return any response times less than 0 or greater than 60s
  AND COALESCE(application__api__response_time_ms,0) > 0
  AND COALESCE(application__api__response_time_ms,0) < 60000
;

SELECT '***** data insert into asp_api_responses_raw complete ******'
;
