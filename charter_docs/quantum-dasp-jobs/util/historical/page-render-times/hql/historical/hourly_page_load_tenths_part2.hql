USE ${env:DASP_db};

set hive.exec.max.dynamic.partitions = 8000;
set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
-- default logic is ignored and Tez tries to group splits into the specified number of groups. Change that parameter carefully.
set tez.grouping.split-count=1200;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/jdatehour-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_datehour AS 'Epoch_Datehour';

SELECT "\n\nFor Cold Page Interactive Time\n\n";
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
                                CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
                                     WHEN visit__application_details__application_name = 'SMB'     THEN 'smb'
                                     WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
                                END      AS domain
                 FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
                WHERE (
                        ( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
                          AND (visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement'))
                      )
                  AND message__name      = 'pageViewPerformance'
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
              CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
                   WHEN visit__application_details__application_name = 'SMB'     THEN 'smb'
                   WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
              END      AS domain
         FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
        WHERE (
                ( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
                  AND (visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement'))
              )
          AND message__name      = 'pageViewPerformance'
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
                                CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
                                     WHEN visit__application_details__application_name = 'SMB'     THEN 'smb'
                                     WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
                                END      AS domain
                 FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
                WHERE (
                        ( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
                          AND (visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement'))
                      )
                  AND message__name      = 'pageViewPerformance'
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
              CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
                   WHEN visit__application_details__application_name = 'SMB'     THEN 'smb'
                   WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
              END      AS domain
         FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
        WHERE (
                ( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
                  AND (visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement'))
              )
          AND message__name      = 'pageViewPerformance'
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
                                CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
                                     WHEN visit__application_details__application_name = 'SMB'     THEN 'smb'
                                     WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
                                END      AS domain
                 FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
                WHERE (( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
                 and visit__application_details__application_name in ('SpecNet', 'SMB', 'IDManagement'))
                  AND message__name = 'pageView'
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
              CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
              WHEN visit__application_details__application_name = 'SMB'     THEN 'smb'
              WHEN visit__application_details__application_name = 'IDManagement' THEN 'idm'
              END      AS domain
         FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
        WHERE (( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
                and visit__application_details__application_name in ('SpecNet', 'SMB', 'IDManagement'))
          AND message__name = 'pageView'
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


--- Keep GLUE and HDFS clean by deleting temporary tables after use
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser_quantum PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_top_browsers_quantum PURGE;
