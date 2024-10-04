USE ${env:ENVIRONMENT};

-- Drop and rebuild asp_hourly_page_load_tenths temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths AS
SELECT  date_denver,
        date_hour_denver,
      --  browser_name,
        page_name,
        pg_load_sec_tenths,
        COUNT(DISTINCT visit__visit_id) as unique_visits,
        COUNT(1) as instances
FROM  (SELECt epoch_datehour(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_hour_denver,
              partition_date as date_denver,
            --  visit__device__browser__name as browser_name, -- TOP 50 browsers
              state__view__current_page__name as page_name,
              ROUND(state__view__current_page__page_load_time/1000,1) as pg_load_sec_tenths,
              visit__visit_id
       FROM asp_v_net_events ne
       WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
       AND message__category = 'Page View'
       AND state__view__current_page__page_load_time > 0
       ) dictionary
GROUP BY date_denver,
         date_hour_denver,
         -- browser_name,
         page_name,
         pg_load_sec_tenths
;

INSERT OVERWRITE TABLE asp_hourly_page_load_tenths PARTITION(date_denver, date_hour_denver, domain)
select page_name,
       pg_load_sec_tenths,
       unique_visits,
       instances,
       date_denver,
       date_hour_denver,
       'Spectrum.net' as domain
FROM ${env:TMP_db}.asp_hourly_page_load_tenths;

-- Drop and rebuild asp_top_browsers temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_top_browsers PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_top_browsers AS
SELECT  date_denver,
        rank_browser,
        browser_name,
        unique_visits,
        'Spectrum.net' as domain
FROM (  SELECT  browser_name,
                COUNT(visit__visit_id) as unique_visits,
                rank() OVER (partition by date_denver order by COUNT(visit__visit_id) DESC)
                 as rank_browser,
                 date_denver
        FROM  ( SELECT DISTINCT visit__device__browser__name as browser_name, -- TOP 50 browsers
                                visit__visit_id,
                                partition_date as date_denver
                FROM asp_v_net_events ne
                WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
                AND message__category = 'Page View'
                AND state__view__current_page__page_load_time > 0
              ) dictionary_1
        GROUP BY browser_name,
                 date_denver
     ) dictionary_2
WHERE rank_browser <= 50
;

INSERT OVERWRITE TABLE asp_top_browsers PARTITION(date_denver, domain)
SELECT rank_browser,
       browser_name,
       unique_visits,
       date_denver,
       'Spectrum.net' as domain
FROM ${env:TMP_db}.asp_top_browsers;

-- Drop and rebuild asp_hourly_page_load_tenths_by_top_browsers temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser AS
SELECT  date_denver,
        date_hour_denver,
        browser_name,
        page_name,
        pg_load_sec_tenths,
        COUNT(DISTINCT visit__visit_id) as unique_visits,
        COUNT(1) as instances
FROM  (SELECt epoch_datehour(cast(message__timestamp * 1000 as bigint),'America/Denver') as date_hour_denver,
              partition_date as date_denver,
              visit__device__browser__name as browser_name,
              state__view__current_page__name as page_name,
              ROUND(state__view__current_page__page_load_time/1000,1) as pg_load_sec_tenths,
              visit__visit_id
       FROM asp_v_net_events ne
       WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
       AND message__category = 'Page View'
       AND state__view__current_page__page_load_time > 0
       ) dictionary
GROUP BY date_denver,
         date_hour_denver,
         browser_name,
         page_name,
         pg_load_sec_tenths
;
INSERT OVERWRITE TABLE asp_hourly_page_load_tenths_by_browser PARTITION (date_denver, date_hour_denver, domain)
select browser_name,
       page_name,
       pg_load_sec_tenths,
       unique_visits,
       instances,
       date_denver,
       date_hour_denver,
       'Spectrum.net' as domain
FROM ${env:TMP_db}.asp_hourly_page_load_tenths_by_browser;
