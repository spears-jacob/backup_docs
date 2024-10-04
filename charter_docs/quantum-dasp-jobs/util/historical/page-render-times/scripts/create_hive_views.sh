#!/bin/bash
# TODO: change enviroment to prod
hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_operational_daily_page_load_times;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_operational_daily_page_load_times AS SELECT * FROM ${DASP_db}.asp_operational_daily_page_views_by_page_load_time;"

hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_page_render_time_seconds_page_views_visits;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_page_render_time_seconds_page_views_visits AS SELECT * FROM ${DASP_db}.asp_page_render_time_seconds_page_views_visits;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_agg_page_load_time_grouping_sets_quantum AS SELECT * FROM ${DASP_db}.asp_agg_page_load_time_grouping_sets_quantum;"

hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_hourly_page_load_tenths_quantum;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_hourly_page_load_tenths_quantum AS SELECT * FROM ${DASP_db}.asp_hourly_page_load_tenths_quantum
WHERE date_denver > date_sub(current_date, 90);"

hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_pg_ld_nulls;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_pg_ld_nulls AS
SELECT * FROM
(SELECT
        epoch_converter(asp_v_venona_events_portals.received__timestamp) as partition_date_denver,
        asp_v_venona_events_portals.visit__device__browser__name as browser_name,
        asp_v_venona_events_portals.state__view__current_page__page_name as auth_unauth_page,
        'Cold' as pg_load_type,
        CASE
              WHEN (state__view__current_page__performance_timing__dom_content_loaded_event_end < 0
                 or state__view__current_page__performance_timing__fetchstart < 0) THEN 'Negative'
              WHEN (state__view__current_page__performance_timing__dom_content_loaded_event_end IS NULL
                 OR state__view__current_page__performance_timing__fetchstart is NULL) THEN 'Null'
              ELSE 'The Rest'
        END AS pg_ld_flag,
        case WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
             WHEN visit__application_details__application_name = 'SMB' THEN 'smb'
        END as domain,
        SIZE(COLLECT_SET(asp_v_venona_events_portals.visit__visit_id)) as unique_visits,
        COUNT(1) as page_views
  FROM ${DASP_db}.asp_v_venona_events_portals
 WHERE asp_v_venona_events_portals.partition_date_hour_utc >= DATE_ADD(CURRENT_DATE,-30)
   AND asp_v_venona_events_portals.message__name = 'pageViewPerformance'
   AND visit__application_details__application_name in ('SpecNet', 'SMB')
   AND (state__view__current_page__performance_timing__dom_content_loaded_event_end < 0
     OR state__view__current_page__performance_timing__fetchstart < 0
     OR state__view__current_page__performance_timing__dom_content_loaded_event_end IS NULL
     OR state__view__current_page__performance_timing__fetchstart IS NULL)
 GROUP BY
        epoch_converter(asp_v_venona_events_portals.received__timestamp),
        asp_v_venona_events_portals.visit__device__browser__name,
        asp_v_venona_events_portals.state__view__current_page__page_name,
        CASE
          WHEN (state__view__current_page__performance_timing__dom_content_loaded_event_end < 0
             or state__view__current_page__performance_timing__fetchstart < 0) THEN 'Negative'
          WHEN (state__view__current_page__performance_timing__dom_content_loaded_event_end IS NULL
             OR state__view__current_page__performance_timing__fetchstart is NULL) THEN 'Null'
          ELSE 'The Rest'
        END,
        case WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
             WHEN visit__application_details__application_name = 'SMB' THEN 'smb'
        END
UNION ALL
SELECT
        epoch_converter(asp_v_venona_events_portals.received__timestamp) as partition_date_denver,
        asp_v_venona_events_portals.visit__device__browser__name as browser_name,
        asp_v_venona_events_portals.state__view__current_page__page_name as auth_unauth_page,
        'Hot' as pg_load_type,
        CASE
              WHEN (state__view__current_page__render_details__fully_rendered_ms < 0) THEN 'Negative'
              WHEN (state__view__current_page__render_details__fully_rendered_ms IS NULL) THEN 'Null'
              ELSE 'The Rest'
        END AS pg_ld_flag,
        case WHEN (visit__application_details__application_name = 'SpecNet') THEN 'Spectrum.net'
             WHEN (visit__application_details__application_name = 'SMB') THEN 'smb'
        END as domain,
        SIZE(COLLECT_SET(asp_v_venona_events_portals.visit__visit_id)) as unique_visits,
        COUNT(1) as page_views
  FROM ${DASP_db}.asp_v_venona_events_portals
 WHERE asp_v_venona_events_portals.partition_date_hour_utc >= DATE_ADD(CURRENT_DATE,-30)
   AND asp_v_venona_events_portals.message__name = 'pageViewPerformance'
   AND visit__application_details__application_name in ('SpecNet', 'SMB')
   AND (state__view__current_page__render_details__fully_rendered_ms < 0
     OR state__view__current_page__render_details__fully_rendered_ms IS NULL)
 GROUP BY
        prod.epoch_converter(asp_v_venona_events_portals.received__timestamp),
        asp_v_venona_events_portals.visit__device__browser__name,
        asp_v_venona_events_portals.state__view__current_page__page_name,
        CASE
          WHEN (state__view__current_page__render_details__fully_rendered_ms < 0) THEN 'Negative'
          WHEN (state__view__current_page__render_details__fully_rendered_ms IS NULL) THEN 'Null'
          ELSE 'The Rest'
        END,
        case WHEN (visit__application_details__application_name = 'SpecNet') THEN 'Spectrum.net'
             WHEN (visit__application_details__application_name = 'SMB') THEN 'smb'
        END) a;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_hourly_page_load_tenths_by_top_browser_quantum as
select  pl.pg_load_type,
        pl.date_denver,
        date_hour_denver,
        pl.domain,
        page_name,
        rank_browser,
        pl.browser_name,
        pg_load_sec_tenths,
        pl.unique_visits,
        instances
FROM ${DASP_db}.asp_hourly_page_load_tenths_by_browser_quantum pl
inner join ${DASP_db}.asp_top_browsers_quantum tb
  ON  pl.date_Denver = tb.date_denver
  AND pl.domain = tb.domain
  AND pl.browser_name = tb.browser_name
  AND pl.pg_load_type = tb.pg_load_type
;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_hourly_page_load_seconds_by_top_browser_quantum as
select  pl.pg_load_type,
        pl.date_denver,
        date_hour_denver,
        pl.domain,
        page_name,
        rank_browser,
        pl.browser_name,
        ROUND(pg_load_sec_tenths,0) as pg_load_sec,
        SUM(pl.unique_visits) as unique_visits,
        SUM(instances) as instances
FROM ${DASP_db}.asp_hourly_page_load_tenths_by_browser_quantum pl
inner join ${DASP_db}.asp_top_browsers_quantum tb
  ON  pl.date_Denver = tb.date_denver
  AND pl.domain = tb.domain
  AND pl.browser_name = tb.browser_name
  AND pl.pg_load_type = tb.pg_load_type
GROUP BY pl.pg_load_type,
         pl.date_denver,
         date_hour_denver,
         pl.domain,
         page_name,
         rank_browser,
         pl.browser_name,
         ROUND(pg_load_sec_tenths,0)
;"
