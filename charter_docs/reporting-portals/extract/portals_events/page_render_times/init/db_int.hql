USE ${env:ENVIRONMENT};

DROP VIEW IF EXISTS asp_v_operational_daily_page_load_times;
CREATE VIEW asp_v_operational_daily_page_load_times AS
  SELECT * FROM prod.asp_operational_daily_page_views_by_page_load_time
;


CREATE table if not exists asp_page_render_time_seconds_page_views_visits
( domain string,
  page_name string,
  count_page_views bigint,
  count_visits int,
  hot_pg_load_sec bigint,
  cold_pg_load_sec bigint)
PARTITIONED BY (
    date_denver string)
;


DROP VIEW IF EXISTS asp_v_page_render_time_seconds_page_views_visits;
CREATE VIEW asp_v_page_render_time_seconds_page_views_visits AS
  SELECT * FROM prod.asp_page_render_time_seconds_page_views_visits
;

CREATE TABLE if not exists asp_agg_page_load_time_grouping_sets_quantum(
  pg_load_type string,
  grouping_id int,
  grouped_partition_date_denver string,
  grouped_partition_date_hour_denver string,
  grouped_browser_name string,
  grouped_breakpoints string,
  partition_date_hour_denver string,
  browser_name string,
  browser_size_breakpoint string,
  page_name string,
  section string,
  domain string,
  unique_visits int,
  page_views bigint,
  avg_pg_ld double,
  total_pg_ld double,
  05_percentile double,
  10_percentile double,
  15_percentile double,
  20_percentile double,
  25_percentile double,
  30_percentile double,
  35_percentile double,
  40_percentile double,
  45_percentile double,
  50_percentile double,
  55_percentile double,
  60_percentile double,
  65_percentile double,
  70_percentile double,
  75_percentile double,
  80_percentile double,
  85_percentile double,
  90_percentile double,
  95_percentile double,
  99_percentile double)
PARTITIONED BY (
  partition_date_denver string);

CREATE VIEW IF NOT EXISTS asp_v_agg_page_load_time_grouping_sets_quantum AS
    SELECT * FROM asp_agg_page_load_time_grouping_sets_quantum
;

CREATE TABLE if not exists asp_hourly_page_load_tenths_quantum(
  pg_load_type string,
  company string,
  device_os string,
  browser_name string,
  page_name string,
  pg_load_sec_tenths double,
  unique_visits bigint,
  instances bigint)
PARTITIONED BY (
  date_denver string,
  date_hour_denver string,
  domain string);

DROP VIEW if EXISTS asp_v_hourly_page_load_tenths_quantum;
CREATE VIEW if NOT EXISTS asp_v_hourly_page_load_tenths_quantum AS
select *
  from asp_hourly_page_load_tenths_quantum
 WHERE date_denver > date_sub(current_date, 90);

CREATE TABLE if not exists asp_top_browsers_quantum(
  pg_load_type string,
  rank_browser int,
  browser_name string,
  unique_visits bigint)
PARTITIONED BY (
  date_denver string,
  domain string);

CREATE TABLE if not exists asp_hourly_page_load_tenths_by_browser_quantum(
  pg_load_type string,
  browser_name string,
  page_name string,
  pg_load_sec_tenths double,
  unique_visits bigint,
  instances bigint)
PARTITIONED BY (
  date_denver string,
  date_hour_denver string,
  domain string);

DROP VIEW if EXISTS asp_v_pg_ld_nulls;
CREATE VIEW if NOT EXISTS asp_v_pg_ld_nulls AS
SELECT * FROM
(SELECT
        prod.epoch_converter(asp_v_venona_events_portals.received__timestamp) as partition_date_denver,
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
        case WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
             WHEN lower(visit__application_details__application_name) = 'smb' THEN 'smb'
        END as domain,
        SIZE(COLLECT_SET(asp_v_venona_events_portals.visit__visit_id)) as unique_visits,
        COUNT(1) as page_views
  FROM asp_v_venona_events_portals
 WHERE asp_v_venona_events_portals.partition_date_hour_utc >= DATE_ADD(CURRENT_DATE,-30)
       -- AND lower(state__view__current_page__name) rlike ".*home.*auth.*"
   AND asp_v_venona_events_portals.message__name = 'pageViewPerformance'
   AND lower(visit__application_details__application_name) in ('specnet', 'smb')
   AND (state__view__current_page__performance_timing__dom_content_loaded_event_end < 0
     OR state__view__current_page__performance_timing__fetchstart < 0
     OR state__view__current_page__performance_timing__dom_content_loaded_event_end IS NULL
     OR state__view__current_page__performance_timing__fetchstart IS NULL)
 GROUP BY
        prod.epoch_converter(asp_v_venona_events_portals.received__timestamp),
        asp_v_venona_events_portals.visit__device__browser__name,
        asp_v_venona_events_portals.state__view__current_page__page_name,
        CASE
          WHEN (state__view__current_page__performance_timing__dom_content_loaded_event_end < 0
             or state__view__current_page__performance_timing__fetchstart < 0) THEN 'Negative'
          WHEN (state__view__current_page__performance_timing__dom_content_loaded_event_end IS NULL
             OR state__view__current_page__performance_timing__fetchstart is NULL) THEN 'Null'
          ELSE 'The Rest'
        END,
        case WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
             WHEN lower(visit__application_details__application_name) = 'smb' THEN 'smb'
        END
UNION ALL
SELECT
        prod.epoch_converter(asp_v_venona_events_portals.received__timestamp) as partition_date_denver,
        asp_v_venona_events_portals.visit__device__browser__name as browser_name,
        asp_v_venona_events_portals.state__view__current_page__page_name as auth_unauth_page,
        'Hot' as pg_load_type,
        CASE
              WHEN (state__view__current_page__render_details__fully_rendered_ms < 0) THEN 'Negative'
              WHEN (state__view__current_page__render_details__fully_rendered_ms IS NULL) THEN 'Null'
              ELSE 'The Rest'
        END AS pg_ld_flag,
        case WHEN (lower(visit__application_details__application_name) = 'specnet') THEN 'Spectrum.net'
             WHEN (lower(visit__application_details__application_name) = 'smb') THEN 'smb'
        END as domain,
        SIZE(COLLECT_SET(asp_v_venona_events_portals.visit__visit_id)) as unique_visits,
        COUNT(1) as page_views
  FROM asp_v_venona_events_portals
 WHERE asp_v_venona_events_portals.partition_date_hour_utc >= DATE_ADD(CURRENT_DATE,-30)
       -- AND lower(state__view__current_page__name) rlike ".*home.*auth.*"
   AND asp_v_venona_events_portals.message__name = 'pageViewPerformance'
   AND lower(visit__application_details__application_name) in ('specnet', 'smb')
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
        case WHEN (lower(visit__application_details__application_name) = 'specnet') THEN 'Spectrum.net'
             WHEN (lower(visit__application_details__application_name) = 'smb') THEN 'smb'
        END) a;

CREATE VIEW IF NOT EXISTS asp_v_hourly_page_load_tenths_by_top_browser_quantum as
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
FROM prod.asp_hourly_page_load_tenths_by_browser_quantum pl
inner join prod.asp_top_browsers_quantum tb
  ON  pl.date_Denver = tb.date_denver
  AND pl.domain = tb.domain
  AND pl.browser_name = tb.browser_name
  AND pl.pg_load_type = tb.pg_load_type
;

CREATE VIEW IF NOT EXISTS asp_v_hourly_page_load_seconds_by_top_browser_quantum as
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
FROM prod.asp_hourly_page_load_tenths_by_browser_quantum pl
inner join prod.asp_top_browsers_quantum tb
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
;
