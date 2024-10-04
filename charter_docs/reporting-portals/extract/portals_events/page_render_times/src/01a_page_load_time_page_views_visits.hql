USE ${env:ENVIRONMENT};

INSERT OVERWRITE TABLE asp_page_render_time_seconds_page_views_visits PARTITION(date_denver)
SELECT *
FROM
(select
        CASE
          WHEN visit__application_details__application_name = 'MySpectrum' THEN 'app'
          WHEN LOWER(visit__application_details__application_name) = 'specnet' THEN 'resi'
          WHEN LOWER(visit__application_details__application_name) =  'smb' THEN 'smb'
          WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
          ELSE 'UNKNOWN'
        END AS domain,
        state__view__current_page__page_name AS page_name,
        SUM(IF(message__name ='pageView', 1, 0)) AS count_page_views,
        SIZE(COLLECT_SET(visit__visit_id))   AS count_visits,
        floor(state__view__current_page__render_details__fully_rendered_ms / 1000) as hot_pg_load_sec,
        -1 AS cold_pg_load_sec,
        prod.epoch_converter(received__timestamp) AS date_denver
 from asp_v_venona_events_portals
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
  AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
  AND state__view__current_page__render_details__fully_rendered_ms > 0
group by  prod.epoch_converter(received__timestamp),
          CASE
            WHEN visit__application_details__application_name = 'MySpectrum' THEN 'app'
            WHEN LOWER(visit__application_details__application_name) = 'specnet' THEN 'resi'
            WHEN LOWER(visit__application_details__application_name) =  'smb' THEN 'smb'
            WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
            ELSE 'UNKNOWN'
          END,
          state__view__current_page__page_name,
          floor(state__view__current_page__render_details__fully_rendered_ms / 1000)
UNION ALL
select
        CASE
          WHEN visit__application_details__application_name = 'MySpectrum' THEN 'app'
          WHEN LOWER(visit__application_details__application_name) = 'specnet' THEN 'resi'
          WHEN LOWER(visit__application_details__application_name) =  'smb' THEN 'smb'
          WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
          ELSE 'UNKNOWN'
        END AS domain,
        state__view__current_page__page_name AS page_name,
        SUM(IF(message__name ='pageViewPerformance', 1, 0)) AS count_page_views,
        SIZE(COLLECT_SET(visit__visit_id))   AS count_visits,
        -1     AS hot_pg_load_sec,
        floor((state__view__current_page__performance_timing__dom_complete-
               state__view__current_page__performance_timing__navigation_start)/1000) as cold_pg_load_sec,
        prod.epoch_converter(received__timestamp) AS date_denver
 from asp_v_venona_events_portals
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
  AND partition_date_hour_utc <  '${env:END_DATE_TZ}')
  AND (state__view__current_page__performance_timing__dom_complete-
         state__view__current_page__performance_timing__navigation_start) > 0
group by  prod.epoch_converter(received__timestamp),
          CASE
            WHEN visit__application_details__application_name = 'MySpectrum' THEN 'app'
            WHEN LOWER(visit__application_details__application_name) = 'specnet' THEN 'resi'
            WHEN LOWER(visit__application_details__application_name) =  'smb' THEN 'smb'
            WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
            ELSE 'UNKNOWN'
          END,
          state__view__current_page__page_name,
          floor((state__view__current_page__performance_timing__dom_complete-
                   state__view__current_page__performance_timing__navigation_start)/1000)
) a;
