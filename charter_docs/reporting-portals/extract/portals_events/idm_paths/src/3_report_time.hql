USE ${env:ENVIRONMENT};

--time spent on page
SELECT "\n\nFor asp_idm_paths_time\n\n";

INSERT OVERWRITE TABLE asp_idm_paths_time PARTITION(date_denver)
select
      LOWER(visit__application_details__application_name) AS platform,
      parse_url(visit__application_details__referrer_link,'HOST') AS referrer_link,
      visit__device__browser__name AS browser_name,
      LOWER(visit__device__device_type) AS device_type,
      state__view__previous_page__page_name as pagename,
      state__view__previous_page__page_viewed_time_ms/1000 as page_viewed_time_sec,
      prod.epoch_converter(received__timestamp, 'America/Denver') as date_denver
 from asp_v_venona_events_portals
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}' and partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND visit__application_details__application_name = 'IDManagement'
  AND message__name ='pageView'
  AND state__view__previous_page__page_name is not null
  AND state__view__previous_page__page_viewed_time_ms > 0
;
