USE ${env:ENVIRONMENT};

set hive.exec.max.dynamic.partitions = 8000;

SELECT "\n\nFor Cold Page Interactive Time\n\n";
-- FOR COLD PAGE Interactive TIME
-- Drop and rebuild asp_hourly_page_load_tenths_quantum temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum AS
SELECT  date_denver,
        date_hour_denver,
        company,
        device_os,
        browser_name,
        page_name,
        pg_load_sec_tenths,
        COUNT(DISTINCT visit__visit_id)   AS unique_visits,
        COUNT(1)       AS instances,
        domain
FROM  (SELECT epoch_datehour(received__timestamp)  AS date_hour_denver,
              epoch_converter(received__timestamp) AS date_denver,
              COALESCE (visit__account__details__mso,'Unknown') AS company,
              split(visit__device__operating_system,"\\.")[0]   AS device_os,
              visit__device__device_type           AS device_type,
              visit__device__browser__name         AS browser_name,
              state__view__current_page__page_name AS page_name,
              ROUND((state__view__current_page__performance_timing__dom_content_loaded_event_end -
                     state__view__current_page__performance_timing__navigation_start)/1000,1) AS pg_load_sec_tenths,
              visit__visit_id,
              CASE WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
                   WHEN lower(visit__application_details__application_name) = 'smb'     THEN 'smb'
                   WHEN lower(visit__application_details__application_name) = 'idmanagement' THEN 'idm'
              END      AS domain
        FROM asp_v_venona_events_portals
       WHERE ( partition_date_hour_utc  >= '${env:START_DATE_TZ}' AND partition_date_hour_utc < '${env:END_DATE_TZ}')
         AND message__name = 'pageViewPerformance'
         and lower(visit__application_details__application_name) in ('specnet', 'smb', 'idmanagement')
         AND (state__view__current_page__performance_timing__dom_content_loaded_event_end -
              state__view__current_page__performance_timing__navigation_start) > 0
       ) dictionary
GROUP BY date_denver,
         date_hour_denver,
         company,
         device_os,
         browser_name,
         page_name,
         pg_load_sec_tenths,
         domain
;

INSERT OVERWRITE TABLE asp_hourly_page_load_tenths_quantum PARTITION(date_denver, date_hour_denver, domain)
SELECT 'Cold Interactive'          AS pg_load_type,
       company,
       device_os,
       browser_name,
       page_name,
       pg_load_sec_tenths,
       unique_visits,
       instances,
       date_denver,
       date_hour_denver,
       domain
FROM ${env:TMP_db}.asp_hourly_page_load_tenths_quantum;

-- Drop and rebuild asp_top_browsers_quantum temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_top_browsers_quantum PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_top_browsers_quantum AS
SELECT  date_denver,
        rank_browser,
        browser_name,
        unique_visits,
        domain
FROM (  SELECT  browser_name,
                COUNT(visit__visit_id)   AS unique_visits,
                rank() OVER (partition by date_denver order by COUNT(visit__visit_id) DESC)
                       AS rank_browser,
                date_denver,
                domain
        FROM  ( SELECT DISTINCT visit__device__browser__name AS browser_name, -- TOP 50 browsers
                                visit__visit_id,
                                epoch_converter(received__timestamp)  AS date_denver,
                                CASE WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
                                     WHEN lower(visit__application_details__application_name) = 'smb' THEN 'smb'
                                     WHEN lower(visit__application_details__application_name) = 'idmanagement' THEN 'idm'
                                END     AS domain
                 FROM asp_v_venona_events_portals
                WHERE ( partition_date_hour_utc  >= '${env:START_DATE_TZ}' AND partition_date_hour_utc < '${env:END_DATE_TZ}')
                  AND message__name      = 'pageViewPerformance'
                  and lower(visit__application_details__application_name) in ('specnet', 'smb', 'idmanagement')
                  AND (state__view__current_page__performance_timing__dom_content_loaded_event_end -
                       state__view__current_page__performance_timing__navigation_start) > 0
              ) dictionary_1
        GROUP BY browser_name,
                 date_denver,
                 domain
     ) dictionary_2
WHERE rank_browser     <= 50
;

INSERT OVERWRITE TABLE asp_top_browsers_quantum PARTITION(date_denver, domain)
SELECT 'Cold Interactive'          AS pg_load_type,
       rank_browser,
       browser_name,
       unique_visits,
       date_denver,
       domain
FROM ${env:TMP_db}.asp_top_browsers_quantum;

-- Drop and rebuild asp_hourly_page_load_tenths_quantum_by_top_browsers temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser_quantum PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser_quantum AS
SELECT  date_denver,
        date_hour_denver,
        browser_name,
        page_name,
        pg_load_sec_tenths,
        COUNT(DISTINCT visit__visit_id)   AS unique_visits,
        COUNT(1)       AS instances,
        domain
FROM  (SELECT epoch_datehour(received__timestamp)   AS date_hour_denver,
              epoch_converter(received__timestamp)  AS date_denver,
              visit__device__browser__name          AS browser_name,
              state__view__current_page__page_name  AS page_name,
              ROUND((state__view__current_page__performance_timing__dom_content_loaded_event_end -
                     state__view__current_page__performance_timing__navigation_start)/1000,1) AS pg_load_sec_tenths,
              visit__visit_id,
              case WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
                   WHEN lower(visit__application_details__application_name) = 'smb' THEN 'smb'
                   WHEN lower(visit__application_details__application_name) = 'idmanagement' THEN 'idm'
              END          AS domain
         FROM asp_v_venona_events_portals
        WHERE ( partition_date_hour_utc  >= '${env:START_DATE_TZ}' AND partition_date_hour_utc < '${env:END_DATE_TZ}')
          AND message__name      = 'pageViewPerformance'
          and lower(visit__application_details__application_name) in ('specnet', 'smb', 'idmanagement')
          AND (state__view__current_page__performance_timing__dom_content_loaded_event_end -
               state__view__current_page__performance_timing__navigation_start) > 0
       ) dictionary
GROUP BY date_denver,
         date_hour_denver,
         browser_name,
         page_name,
         pg_load_sec_tenths,
         domain
;

INSERT OVERWRITE TABLE asp_hourly_page_load_tenths_by_browser_quantum PARTITION (date_denver, date_hour_denver, domain)
select 'Cold Interactive'          AS pg_load_type,
       browser_name,
       page_name,
       pg_load_sec_tenths,
       unique_visits,
       instances,
       date_denver,
       date_hour_denver,
       domain
FROM ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser_quantum;

SELECT "\n\nFor Cold Page Fully Loaded Time\n\n";
-- FOR COLD PAGE Fully Loaded TIME
-- Drop and rebuild asp_hourly_page_load_tenths_quantum temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum AS
SELECT  date_denver,
        date_hour_denver,
        company,
        device_os,
        browser_name,
        page_name,
        pg_load_sec_tenths,
        COUNT(DISTINCT visit__visit_id)   AS unique_visits,
        COUNT(1)       AS instances,
        domain
FROM  (SELECT epoch_datehour(received__timestamp)  AS date_hour_denver,
              epoch_converter(received__timestamp) AS date_denver,
              COALESCE (visit__account__details__mso,'Unknown') AS company,
              split(visit__device__operating_system,"\\.")[0]   AS device_os,
              visit__device__device_type           AS device_type,
              visit__device__browser__name         AS browser_name,
              state__view__current_page__page_name AS page_name,
              ROUND((state__view__current_page__performance_timing__dom_complete -
                     state__view__current_page__performance_timing__navigation_start)/1000,1) AS pg_load_sec_tenths,
              visit__visit_id,
              CASE WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
                   WHEN lower(visit__application_details__application_name) = 'smb'     THEN 'smb'
                   WHEN lower(visit__application_details__application_name) = 'idmanagement' THEN 'idm'
              END      AS domain
        FROM asp_v_venona_events_portals
       WHERE ( partition_date_hour_utc  >= '${env:START_DATE_TZ}' AND partition_date_hour_utc < '${env:END_DATE_TZ}')
         AND message__name = 'pageViewPerformance'
         and lower(visit__application_details__application_name) in ('specnet', 'smb', 'idmanagement')
         AND (state__view__current_page__performance_timing__dom_complete -
              state__view__current_page__performance_timing__navigation_start) > 0
       ) dictionary
GROUP BY date_denver,
         date_hour_denver,
         company,
         device_os,
         browser_name,
         page_name,
         pg_load_sec_tenths,
         domain
;

INSERT INTO TABLE asp_hourly_page_load_tenths_quantum PARTITION(date_denver, date_hour_denver, domain)
SELECT 'Cold Fully Loaded'          AS pg_load_type,
       company,
       device_os,
       browser_name,
       page_name,
       pg_load_sec_tenths,
       unique_visits,
       instances,
       date_denver,
       date_hour_denver,
       domain
FROM ${env:TMP_db}.asp_hourly_page_load_tenths_quantum;

-- Drop and rebuild asp_top_browsers_quantum temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_top_browsers_quantum PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_top_browsers_quantum AS
SELECT  date_denver,
        rank_browser,
        browser_name,
        unique_visits,
        domain
FROM (  SELECT  browser_name,
                COUNT(visit__visit_id)   AS unique_visits,
                rank() OVER (partition by date_denver order by COUNT(visit__visit_id) DESC)
                       AS rank_browser,
                date_denver,
                domain
        FROM  ( SELECT DISTINCT visit__device__browser__name AS browser_name, -- TOP 50 browsers
                                visit__visit_id,
                                epoch_converter(received__timestamp)  AS date_denver,
                                CASE WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
                                     WHEN lower(visit__application_details__application_name) = 'smb' THEN 'smb'
                                     WHEN lower(visit__application_details__application_name) = 'idmanagement' THEN 'idm'
                                END     AS domain
                 FROM asp_v_venona_events_portals
                WHERE ( partition_date_hour_utc  >= '${env:START_DATE_TZ}' AND partition_date_hour_utc < '${env:END_DATE_TZ}')
                  AND message__name      = 'pageViewPerformance'
                  and lower(visit__application_details__application_name) in ('specnet', 'smb', 'idmanagement')
                  AND (state__view__current_page__performance_timing__dom_complete -
                       state__view__current_page__performance_timing__navigation_start) > 0
              ) dictionary_1
        GROUP BY browser_name,
                 date_denver,
                 domain
     ) dictionary_2
WHERE rank_browser     <= 50
;

INSERT INTO TABLE asp_top_browsers_quantum PARTITION(date_denver, domain)
SELECT 'Cold Fully Loaded'          AS pg_load_type,
       rank_browser,
       browser_name,
       unique_visits,
       date_denver,
       domain
FROM ${env:TMP_db}.asp_top_browsers_quantum;

-- Drop and rebuild asp_hourly_page_load_tenths_quantum_by_top_browsers temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser_quantum PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser_quantum AS
SELECT  date_denver,
        date_hour_denver,
        browser_name,
        page_name,
        pg_load_sec_tenths,
        COUNT(DISTINCT visit__visit_id)   AS unique_visits,
        COUNT(1)       AS instances,
        domain
FROM  (SELECT epoch_datehour(received__timestamp)   AS date_hour_denver,
              epoch_converter(received__timestamp)  AS date_denver,
              visit__device__browser__name          AS browser_name,
              state__view__current_page__page_name  AS page_name,
              ROUND((state__view__current_page__performance_timing__dom_complete -
                     state__view__current_page__performance_timing__navigation_start)/1000,1) AS pg_load_sec_tenths,
              visit__visit_id,
              case WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
                   WHEN lower(visit__application_details__application_name) = 'smb' THEN 'smb'
                   WHEN lower(visit__application_details__application_name) = 'idmanagement' THEN 'idm'
              END          AS domain
         FROM asp_v_venona_events_portals
        WHERE ( partition_date_hour_utc  >= '${env:START_DATE_TZ}' AND partition_date_hour_utc < '${env:END_DATE_TZ}')
          AND message__name      = 'pageViewPerformance'
          and lower(visit__application_details__application_name) in ('specnet', 'smb', 'idmanagement')
          AND (state__view__current_page__performance_timing__dom_complete -
               state__view__current_page__performance_timing__navigation_start) > 0
       ) dictionary
GROUP BY date_denver,
         date_hour_denver,
         browser_name,
         page_name,
         pg_load_sec_tenths,
         domain
;

INSERT INTO TABLE asp_hourly_page_load_tenths_by_browser_quantum PARTITION (date_denver, date_hour_denver, domain)
select 'Cold Fully Loaded'          AS pg_load_type,
       browser_name,
       page_name,
       pg_load_sec_tenths,
       unique_visits,
       instances,
       date_denver,
       date_hour_denver,
       domain
FROM ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser_quantum;

SELECT "\n\nFor Hot Page Load Time\n\n";
-- FOR HOT PAGE LOAD TIME

-- Drop and rebuild asp_hourly_page_load_tenths_quantum temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_quantum AS
SELECT  date_denver,
        date_hour_denver,
        company,
        device_os,
        browser_name,
        page_name,
        pg_load_sec_tenths,
        COUNT(DISTINCT visit__visit_id) AS unique_visits,
        COUNT(1)       AS instances,
        domain
FROM  (SELECt epoch_datehour(received__timestamp)    AS date_hour_denver,
              epoch_converter(received__timestamp)   AS date_denver,
              COALESCE (visit__account__details__mso,'Unknown') AS company,
              split(visit__device__operating_system,"\\.")[0]   AS device_os,
              visit__device__device_type     AS device_type,
              visit__device__browser__name   AS browser_name,
              state__view__current_page__page_name AS page_name,
              ROUND(state__view__current_page__render_details__fully_rendered_ms/1000,1) AS pg_load_sec_tenths,
              visit__visit_id,
              case WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
                   WHEN lower(visit__application_details__application_name) = 'smb'     THEN 'smb'
                   WHEN lower(visit__application_details__application_name) = 'idmanagement' THEN 'idm'
              END      AS domain
        FROM asp_v_venona_events_portals
       WHERE ( partition_date_hour_utc  >= '${env:START_DATE_TZ}' AND partition_date_hour_utc < '${env:END_DATE_TZ}')
         AND message__name = 'pageView'
         and lower(visit__application_details__application_name) in ('specnet', 'smb', 'idmanagement')
         AND state__view__current_page__render_details__fully_rendered_ms > 0
       ) dictionary
GROUP BY date_denver,
         date_hour_denver,
         company,
         device_os,
         browser_name,
         page_name,
         pg_load_sec_tenths,
         domain
;

INSERT INTO TABLE asp_hourly_page_load_tenths_quantum PARTITION(date_denver, date_hour_denver, domain)
select 'Hot'           AS pg_load_type,
       company,
       device_os,
       browser_name,
       page_name,
       pg_load_sec_tenths,
       unique_visits,
       instances,
       date_denver,
       date_hour_denver,
       domain
FROM ${env:TMP_db}.asp_hourly_page_load_tenths_quantum;

-- Drop and rebuild asp_top_browsers_quantum temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_top_browsers_quantum PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_top_browsers_quantum AS
SELECT  date_denver,
        rank_browser,
        browser_name,
        unique_visits,
        domain
FROM (  SELECT  browser_name,
                COUNT(visit__visit_id)    AS unique_visits,
                rank() OVER (partition by date_denver order by COUNT(visit__visit_id) DESC)
                       AS rank_browser,
                 date_denver,
                 domain
        FROM  ( SELECT DISTINCT visit__device__browser__name    AS browser_name, -- TOP 50 browsers
                                visit__visit_id,
                                epoch_converter(received__timestamp)  AS date_denver,
                                case WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
                                     WHEN lower(visit__application_details__application_name) = 'smb'     THEN 'smb'
                                     WHEN lower(visit__application_details__application_name) = 'idmanagement' THEN 'idm'
                                END    AS domain
                 FROM asp_v_venona_events_portals
                WHERE ( partition_date_hour_utc  >= '${env:START_DATE_TZ}' AND partition_date_hour_utc < '${env:END_DATE_TZ}')
                  AND message__name = 'pageView'
                  and lower(visit__application_details__application_name) in ('specnet', 'smb', 'idmanagement')
                  AND state__view__current_page__render_details__fully_rendered_ms > 0
              ) dictionary_1
        GROUP BY browser_name,
                 date_denver,
                 domain
     ) dictionary_2
WHERE rank_browser     <= 50
;

INSERT INTO TABLE asp_top_browsers_quantum PARTITION(date_denver, domain)
SELECT 'Hot'            AS pg_load_type,
       rank_browser,
       browser_name,
       unique_visits,
       date_denver,
       domain
FROM ${env:TMP_db}.asp_top_browsers_quantum;

-- Drop and rebuild asp_hourly_page_load_tenths_quantum_by_top_browsers temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser_quantum PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser_quantum AS
SELECT  date_denver,
        date_hour_denver,
        browser_name,
        page_name,
        pg_load_sec_tenths,
        COUNT(DISTINCT visit__visit_id) AS unique_visits,
        COUNT(1)       AS instances,
        domain
FROM  (SELECt epoch_datehour(received__timestamp)   AS date_hour_denver,
              epoch_converter(received__timestamp)  AS date_denver,
              visit__device__browser__name          AS browser_name,
              state__view__current_page__page_name  AS page_name,
              ROUND(state__view__current_page__render_details__fully_rendered_ms/1000,1) AS pg_load_sec_tenths,
              visit__visit_id,
              case WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
                   WHEN lower(visit__application_details__application_name) = 'smb' THEN 'smb'
                   WHEN lower(visit__application_details__application_name) = 'idmanagement' THEN 'idm'
              END      AS domain
         FROM asp_v_venona_events_portals
        WHERE ( partition_date_hour_utc  >= '${env:START_DATE_TZ}' AND partition_date_hour_utc < '${env:END_DATE_TZ}')
          AND message__name = 'pageView'
          and lower(visit__application_details__application_name) in ('specnet', 'smb', 'idmanagement')
          AND state__view__current_page__render_details__fully_rendered_ms > 0
       ) dictionary
GROUP BY date_denver,
         date_hour_denver,
         browser_name,
         page_name,
         pg_load_sec_tenths,
         domain
;

INSERT INTO TABLE asp_hourly_page_load_tenths_by_browser_quantum PARTITION (date_denver, date_hour_denver, domain)
select 'Hot'           AS pg_load_type,
       browser_name,
       page_name,
       pg_load_sec_tenths,
       unique_visits,
       instances,
       date_denver,
       date_hour_denver,
       domain
FROM ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser_quantum;
