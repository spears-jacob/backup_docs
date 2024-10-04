USE ${env:ENVIRONMENT};

--Count of page views by page (instances)
SELECT "\n\nFor pageview metric\n\n";

INSERT OVERWRITE TABLE asp_idm_paths_metrics PARTITION(date_denver)
select
      LOWER(visit__application_details__application_name) AS platform,
      parse_url(visit__application_details__referrer_link,'HOST') AS referrer_link,
      visit__device__browser__name AS browser_name,
      LOWER(visit__device__device_type) AS device_type,
      state__view__current_page__page_name as pagename,
      state__view__current_page__app_section as app_section,
      state__view__current_page__elements__standardized_name as std_name,
      '' as api_code,
      '' as api_text,
      'pageview' as metric_name,
      count(1) as metric_value,
      prod.epoch_converter(received__timestamp, 'America/Denver') as date_denver
 from asp_v_venona_events_portals
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}' and partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND visit__application_details__application_name = 'IDManagement'
  AND message__name ='pageView'
group by
      prod.epoch_converter(received__timestamp, 'America/Denver'),
      visit__application_details__application_name,
      parse_url(visit__application_details__referrer_link,'HOST'),
      visit__device__browser__name,
      visit__device__device_type,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name;

--Button Clicks for all pages (instances) selectAction includes link
SELECT "\n\nFor buttonClick metric\n\n";

INSERT INTO TABLE asp_idm_paths_metrics PARTITION(date_denver)
select
      LOWER(visit__application_details__application_name) AS platform,
      parse_url(visit__application_details__referrer_link,'HOST') AS referrer_link,
      visit__device__browser__name AS browser_name,
      LOWER(visit__device__device_type) AS device_type,
      state__view__current_page__page_name as pagename,
      state__view__current_page__app_section as app_section,
      state__view__current_page__elements__standardized_name as std_name,
      '' as api_code,
      '' as api_text,
      'buttonClick' as metric_name,
      count(1) as metric_value,
      prod.epoch_converter(received__timestamp, 'America/Denver') as date_denver
 from asp_v_venona_events_portals
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}' and partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND visit__application_details__application_name = 'IDManagement'
  AND message__name ='selectAction'
  AND operation__operation_type = 'buttonClick'
group by
      prod.epoch_converter(received__timestamp, 'America/Denver'),
      visit__application_details__application_name,
      parse_url(visit__application_details__referrer_link,'HOST'),
      visit__device__browser__name,
      visit__device__device_type,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name
;

--API Failure Code and Failure Message
SELECT "\n\nFor api_response metric\n\n";

INSERT INTO TABLE asp_idm_paths_metrics PARTITION(date_denver)
select
      LOWER(visit__application_details__application_name) AS platform,
      parse_url(visit__application_details__referrer_link,'HOST') AS referrer_link,
      visit__device__browser__name AS browser_name,
      LOWER(visit__device__device_type) AS device_type,
      state__view__current_page__page_name as pagename,
      state__view__current_page__app_section as app_section,
      state__view__current_page__elements__standardized_name as std_name,
      application__api__response_code as api_code,
      application__api__response_text as api_text,
      'api_response' as metric_name,
      count(1) as metric_value,
      prod.epoch_converter(received__timestamp, 'America/Denver') as date_denver
 from asp_v_venona_events_portals
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}' and partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND visit__application_details__application_name = 'IDManagement'
  AND application__api__response_code != '200'
group by
      prod.epoch_converter(received__timestamp, 'America/Denver'),
      visit__application_details__application_name,
      parse_url(visit__application_details__referrer_link,'HOST'),
      visit__device__browser__name,
      visit__device__device_type,
      application__api__response_code,
      application__api__response_text,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name
;
