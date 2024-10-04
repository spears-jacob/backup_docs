USE ${env:DASP_db};

SET ProcessingLag=31;

set hive.optimize.sort.dynamic.partition = false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
-- default logic is ignored and Tez tries to group splits into the specified number of groups. Change that parameter carefully.
set tez.grouping.split-count=1200;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/jdatehour-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_datehour AS 'Epoch_Datehour';


SELECT "\n\nThe ProcessingLag variable is used to set how long to lag the page load time,
\n\n now set to ${hiveconf:ProcessingLag} days.\n\n";

-- Page Load Time
-- Aggregate measures
-- - Percentiles
-- - Average
-- - Count, Total
-- Calculated for grouping sets
-- - Date, Date Hour
-- - Browser
-- - Page name (home-unauth, home-authenticated)
-- with all above, including Equipment and billing pages:  540726 rows in 160 seconds
-- by adding browser_size_breakpoint: 643285 rows in 321 seconds
-- by adding section: 645501 rows in 400 seconds
-- Drop and rebuild asp_agg_page_load_time_grouping_sets temp table --


-- FOR COLD PAGE LOAD TIME
SELECT "\n\nFor Cold Page Load Time\n\n";
DROP TABLE IF EXISTS ${env:TMP_db}.asp_agg_page_load_time_grouping_sets_quantum PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_agg_page_load_time_grouping_sets_quantum AS
SELECT  grouping_id,
         -- Dimensions
        IF((grouping_id & 64) = 0, partition_date_denver, "All Dates") as grouped_partition_date_denver,
        IF((grouping_id & 32) = 0, partition_date_hour_denver, "All Hours") as grouped_partition_date_hour_denver,
        IF((grouping_id & 16) = 0, browser_name, "All Browsers") as grouped_browser_name,
        IF((grouping_id & 8)  = 0, browser_size_breakpoint, "All Breakpoints") as grouped_breakpoints,
        partition_date_denver,
        partition_date_hour_denver,
        browser_name,
        browser_size_breakpoint,
        page_name,
        section,
        domain,
        -- Measures and Aggregates
        unique_visits,
        page_views, -- count of instances
        ROUND(avg_pg_ld / 1000,5) as avg_pg_ld,
        ROUND(total_pg_ld / 1000,5) as total_pg_ld,
        ROUND(a[0]  / 1000,5) AS 05_PERCENTILE,  ROUND(a[1]  / 1000,5) AS 10_PERCENTILE,
        ROUND(a[2]  / 1000,5) AS 15_PERCENTILE,  ROUND(a[3]  / 1000,5) AS 20_PERCENTILE,
        ROUND(a[4]  / 1000,5) AS 25_PERCENTILE,  ROUND(a[5]  / 1000,5) AS 30_PERCENTILE,
        ROUND(a[6]  / 1000,5) AS 35_PERCENTILE,  ROUND(a[7]  / 1000,5) AS 40_PERCENTILE,
        ROUND(a[8]  / 1000,5) AS 45_PERCENTILE,  ROUND(a[9]  / 1000,5) AS 50_PERCENTILE,
        ROUND(a[10] / 1000,5) AS 55_PERCENTILE,  ROUND(a[11] / 1000,5) AS 60_PERCENTILE,
        ROUND(a[12] / 1000,5) AS 65_PERCENTILE,  ROUND(a[13] / 1000,5) AS 70_PERCENTILE,
        ROUND(a[14] / 1000,5) AS 75_PERCENTILE,  ROUND(a[15] / 1000,5) AS 80_PERCENTILE,
        ROUND(a[16] / 1000,5) AS 85_PERCENTILE,  ROUND(a[17] / 1000,5) AS 90_PERCENTILE,
        ROUND(a[18] / 1000,5) AS 95_PERCENTILE,  ROUND(a[19] / 1000,5) AS 99_PERCENTILE
FROM -- p_tiles
    ( SELECT  CAST(grouping__id as INT) as grouping_id,
              -- Dimensions -----------
              partition_date_denver,
              partition_date_hour_denver,
              browser_name,
              browser_size_breakpoint,
              page_name,
              section,
              domain,
              -- Measures and Aggregates -----------
              SIZE(COLLECT_SET(visit_id)) as unique_visits,
              COUNT(*) as page_views,
              AVG(page_load_time_ms) as avg_pg_ld,
              sum(page_load_time_ms) as total_pg_ld,
              PERCENTILE(
                page_load_time_ms,
                array(0.05,0.10,0.15,0.20,0.25,0.30,0.35,0.40,0.45,0.5,0.55,0.60,0.65,0.70,0.75,0.80,0.85,0.90,0.95,0.99)) AS a
      FROM ( -- dictionary
            SELECT epoch_converter(received__timestamp) as partition_date_denver,
                   epoch_datehour(received__timestamp) as partition_date_hour_denver,
                   state__view__current_page__ui_responsive_breakpoint as browser_size_breakpoint,
                   state__view__current_page__page_name AS page_name,
                   state__view__current_page__page_id AS page_id,
                   (state__view__current_page__performance_timing__dom_complete-
                    state__view__current_page__performance_timing__navigation_start) as page_load_time_ms,
                   visit__device__browser__name as browser_name,
                   visit__visit_id AS visit_id,
                   CASE WHEN ((visit__application_details__application_name = 'SpecNet' and
                              state__view__current_page__page_name = 'billing-and-transactions') OR
                             (visit__application_details__application_name = 'SMB' and
                                         state__view__current_page__page_name = 'billingHome'))
                              THEN 'Billing'
                        WHEN (visit__application_details__application_name = 'SpecNet' and
                              state__view__current_page__page_name = 'my-tv-services')
                              THEN 'Equipment - TV'
                        WHEN (visit__application_details__application_name = 'SpecNet' and
                              state__view__current_page__page_name = 'my-internet-services')
                              THEN 'Equipment - Internet'
                        WHEN (visit__application_details__application_name = 'SpecNet' and
                              state__view__current_page__page_name = 'my-voice-services')
                              THEN 'Equipment - Voice'
                        WHEN ((visit__application_details__application_name = 'SpecNet' and
                              state__view__current_page__page_name = 'home-authenticated') OR
                             (visit__application_details__application_name = 'SMB' and
                                          state__view__current_page__page_name = 'homeAuth'))
                              THEN 'Home - Authenticated'
                        WHEN ((visit__application_details__application_name = 'SpecNet' and
                              state__view__current_page__page_name = 'home-unauth') OR
                             (visit__application_details__application_name = 'SMB' and
                                           state__view__current_page__page_name = 'homeUnauth'))
                              THEN 'Home - Unauthenticated'
                        ELSE 'Unspecified'
                   END AS section,
                   CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
                        WHEN visit__application_details__application_name = 'SMB' THEN 'smb'
                   END AS domain
            FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
            WHERE
            (( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
            AND (visit__application_details__application_name IN ('SpecNet','SMB','MySpectrum','IDManagement', 'PrivacyMicrosite'))
            )
                AND message__name = 'pageViewPerformance'
                AND (state__view__current_page__performance_timing__dom_complete-
                     state__view__current_page__performance_timing__navigation_start) > 0
                AND (state__view__current_page__performance_timing__dom_complete-
                     state__view__current_page__performance_timing__navigation_start) < 600000 -- 10 mins
                AND state__view__current_page__page_name IN ('my-tv-services',
                                                           'my-internet-services',
                                                           'my-voice-services',
                                                           'home-authenticated',
                                                           'home-unauth',
                                                           'billing-and-transactions',
                                                           'billingHome'
                                                           'homeUnauth',
                                                           'homeAuth')
           ) dictionary
      GROUP BY
                partition_date_denver,
                partition_date_hour_denver,
                browser_name,
                browser_size_breakpoint,
                page_name,
                section,
                domain
      GROUPING SETS ( (domain, page_name),
                      (domain, browser_name, page_name),
                      (domain, browser_name, section, page_name),
                      (domain, partition_date_denver, browser_name, page_name),
                      (domain, partition_date_denver, browser_name, browser_size_breakpoint, page_name),
                      (domain, partition_date_denver, section, browser_size_breakpoint, page_name),
                      (domain, partition_date_denver, page_name),
                      (domain, partition_date_denver, partition_date_hour_denver, page_name),
                      (domain, partition_date_denver, partition_date_hour_denver, browser_name, page_name)
                    ) -- GROUPING SETS
) p_tiles
;

INSERT OVERWRITE TABLE asp_agg_page_load_time_grouping_sets_quantum PARTITION(partition_date_denver)
select  'Cold' as pg_load_type,
        grouping_id,
        grouped_partition_date_denver,
        grouped_partition_date_hour_denver,
        grouped_browser_name,
        grouped_breakpoints,
        partition_date_hour_denver,
        browser_name,
        browser_size_breakpoint,
        page_name,
        section,
        domain,
        unique_visits,
        page_views,
        avg_pg_ld,
        total_pg_ld,
        05_percentile,
        10_percentile,
        15_percentile,
        20_percentile,
        25_percentile,
        30_percentile,
        35_percentile,
        40_percentile,
        45_percentile,
        50_percentile,
        55_percentile,
        60_percentile,
        65_percentile,
        70_percentile,
        75_percentile,
        80_percentile,
        85_percentile,
        90_percentile,
        95_percentile,
        99_percentile,
        partition_date_denver
  FROM ${env:TMP_db}.asp_agg_page_load_time_grouping_sets_quantum;

  SELECT "\n\nFor Hot Page Load Time\n\n";
  -- FOR HOT PAGE LOAD TIME
  DROP TABLE IF EXISTS ${env:TMP_db}.asp_agg_page_load_time_grouping_sets_quantum PURGE;

  -- For troubleshooting queries, remove 'TEMPORARY'
  CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_agg_page_load_time_grouping_sets_quantum AS
  SELECT  grouping_id,
           -- Dimensions
          IF((grouping_id & 64) = 0, partition_date_denver, "All Dates") as grouped_partition_date_denver,
          IF((grouping_id & 32) = 0, partition_date_hour_denver, "All Hours") as grouped_partition_date_hour_denver,
          IF((grouping_id & 16) = 0, browser_name, "All Browsers") as grouped_browser_name,
          IF((grouping_id & 8)  = 0, browser_size_breakpoint, "All Breakpoints") as grouped_breakpoints,
          partition_date_denver,
          partition_date_hour_denver,
          browser_name,
          browser_size_breakpoint,
          page_name,
          section,
          domain,
          -- Measures and Aggregates
          unique_visits,
          page_views, -- count of instances
          ROUND(avg_pg_ld / 1000,5) as avg_pg_ld,
          ROUND(total_pg_ld / 1000,5) as total_pg_ld,
          ROUND(a[0]  / 1000,5) AS 05_PERCENTILE,  ROUND(a[1]  / 1000,5) AS 10_PERCENTILE,
          ROUND(a[2]  / 1000,5) AS 15_PERCENTILE,  ROUND(a[3]  / 1000,5) AS 20_PERCENTILE,
          ROUND(a[4]  / 1000,5) AS 25_PERCENTILE,  ROUND(a[5]  / 1000,5) AS 30_PERCENTILE,
          ROUND(a[6]  / 1000,5) AS 35_PERCENTILE,  ROUND(a[7]  / 1000,5) AS 40_PERCENTILE,
          ROUND(a[8]  / 1000,5) AS 45_PERCENTILE,  ROUND(a[9]  / 1000,5) AS 50_PERCENTILE,
          ROUND(a[10] / 1000,5) AS 55_PERCENTILE,  ROUND(a[11] / 1000,5) AS 60_PERCENTILE,
          ROUND(a[12] / 1000,5) AS 65_PERCENTILE,  ROUND(a[13] / 1000,5) AS 70_PERCENTILE,
          ROUND(a[14] / 1000,5) AS 75_PERCENTILE,  ROUND(a[15] / 1000,5) AS 80_PERCENTILE,
          ROUND(a[16] / 1000,5) AS 85_PERCENTILE,  ROUND(a[17] / 1000,5) AS 90_PERCENTILE,
          ROUND(a[18] / 1000,5) AS 95_PERCENTILE,  ROUND(a[19] / 1000,5) AS 99_PERCENTILE
  FROM -- p_tiles
      ( SELECT  CAST(grouping__id as INT) as grouping_id,
                -- Dimensions -----------
                partition_date_denver,
                partition_date_hour_denver,
                browser_name,
                browser_size_breakpoint,
                page_name,
                section,
                domain,
                -- Measures and Aggregates -----------
                SIZE(COLLECT_SET(visit_id)) as unique_visits,
                COUNT(*) as page_views,
                AVG(page_load_time_ms) as avg_pg_ld,
                sum(page_load_time_ms) as total_pg_ld,
                PERCENTILE(
                  page_load_time_ms,
                  array(0.05,0.10,0.15,0.20,0.25,0.30,0.35,0.40,0.45,0.5,0.55,0.60,0.65,0.70,0.75,0.80,0.85,0.90,0.95,0.99)) AS a
        FROM ( -- dictionary
              SELECT epoch_converter(received__timestamp) as partition_date_denver,
                     epoch_datehour(received__timestamp) as partition_date_hour_denver,
                     state__view__current_page__ui_responsive_breakpoint as browser_size_breakpoint,
                     state__view__current_page__page_name AS page_name,
                     state__view__current_page__page_id AS page_id,
                     state__view__current_page__render_details__fully_rendered_ms AS page_load_time_ms,
                     visit__device__browser__name as browser_name,
                     visit__visit_id AS visit_id,
                     CASE WHEN ((visit__application_details__application_name = 'SpecNet' and
                                state__view__current_page__page_name = 'billing-and-transactions') OR
                               (visit__application_details__application_name = 'SMB' and
                                           state__view__current_page__page_name = 'billingHome'))
                                THEN 'Billing'
                          WHEN (visit__application_details__application_name = 'SpecNet' and
                                state__view__current_page__page_name = 'my-tv-services')
                                THEN 'Equipment - TV'
                          WHEN (visit__application_details__application_name = 'SpecNet' and
                                state__view__current_page__page_name = 'my-internet-services')
                                THEN 'Equipment - Internet'
                          WHEN (visit__application_details__application_name = 'SpecNet' and
                                state__view__current_page__page_name = 'my-voice-services')
                                THEN 'Equipment - Voice'
                          WHEN ((visit__application_details__application_name = 'SpecNet' and
                                state__view__current_page__page_name = 'home-authenticated') OR
                               (visit__application_details__application_name = 'SMB' and
                                            state__view__current_page__page_name = 'homeAuth'))
                                THEN 'Home - Authenticated'
                          WHEN ((visit__application_details__application_name = 'SpecNet' and
                                state__view__current_page__page_name = 'home-unauth') OR
                               (visit__application_details__application_name = 'SMB' and
                                             state__view__current_page__page_name = 'homeUnauth'))
                                THEN 'Home - Unauthenticated'
                          ELSE 'Unspecified'
                     END AS section,
                     CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
                          WHEN visit__application_details__application_name = 'SMB' THEN 'smb'
                     END AS domain
              FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
              WHERE (( partition_date_hour_utc  >= '${hiveconf:START_DATE_TZ}' AND partition_date_hour_utc < '${hiveconf:END_DATE_TZ}')
                      AND visit__application_details__application_name in ('SpecNet','SMB'))
                  AND message__name = 'pageView'
                  AND state__view__current_page__render_details__fully_rendered_ms > 0
                  AND state__view__current_page__render_details__fully_rendered_ms < 600000 -- 10 mins
                  AND state__view__current_page__page_name In('my-tv-services',
                                                            'my-internet-services',
                                                            'my-voice-services',
                                                            'home-authenticated',
                                                            'home-unauth',
                                                            'billing-and-transactions',
                                                            'billingHome'
                                                            'homeUnauth',
                                                            'homeAuth')
             ) dictionary
        GROUP BY
                  partition_date_denver,
                  partition_date_hour_denver,
                  browser_name,
                  browser_size_breakpoint,
                  page_name,
                  section,
                  domain
        GROUPING SETS ( (domain, page_name),
                        (domain, browser_name, page_name),
                        (domain, browser_name, section, page_name),
                        (domain, partition_date_denver, browser_name, page_name),
                        (domain, partition_date_denver, browser_name, browser_size_breakpoint, page_name),
                        (domain, partition_date_denver, section, browser_size_breakpoint, page_name),
                        (domain, partition_date_denver, page_name),
                        (domain, partition_date_denver, partition_date_hour_denver, page_name),
                        (domain, partition_date_denver, partition_date_hour_denver, browser_name, page_name)
                      ) -- GROUPING SETS
  ) p_tiles
  ;

  INSERT INTO TABLE asp_agg_page_load_time_grouping_sets_quantum PARTITION(partition_date_denver)
  select  'Hot' as pg_load_type,
          grouping_id,
          grouped_partition_date_denver,
          grouped_partition_date_hour_denver,
          grouped_browser_name,
          grouped_breakpoints,
          partition_date_hour_denver,
          browser_name,
          browser_size_breakpoint,
          page_name,
          section,
          domain,
          unique_visits,
          page_views,
          avg_pg_ld,
          total_pg_ld,
          05_percentile,
          10_percentile,
          15_percentile,
          20_percentile,
          25_percentile,
          30_percentile,
          35_percentile,
          40_percentile,
          45_percentile,
          50_percentile,
          55_percentile,
          60_percentile,
          65_percentile,
          70_percentile,
          75_percentile,
          80_percentile,
          85_percentile,
          90_percentile,
          95_percentile,
          99_percentile,
          partition_date_denver
    FROM ${env:TMP_db}.asp_agg_page_load_time_grouping_sets_quantum;
