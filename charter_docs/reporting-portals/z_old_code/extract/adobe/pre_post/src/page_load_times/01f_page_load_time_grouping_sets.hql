USE ${env:ENVIRONMENT};

SET ProcessingLag=31;

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

DROP TABLE IF EXISTS ${env:TMP_db}.asp_agg_page_load_time_grouping_sets PURGE;

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.asp_agg_page_load_time_grouping_sets AS
SELECT  grouping_id,
         -- Dimensions
        IF((grouping_id & 1) != 0, partition_date_denver, "All Dates") as grouped_partition_date_denver,
        IF((grouping_id & 2) != 0, partition_date_hour_denver, "All Hours") as grouped_partition_date_hour_denver,
        IF((grouping_id & 4) != 0, browser_name, "All Browsers") as grouped_browser_name,
        IF((grouping_id & 8) != 0, browser_size_breakpoint, "All Breakpoints") as grouped_breakpoints,
        partition_date_denver,
        partition_date_hour_denver,
        browser_name,
        browser_size_breakpoint,
        page_name,
        section,
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
              -- Measures and Aggregates -----------
              SIZE(COLLECT_SET(visit_id)) as unique_visits,
              COUNT(*) as page_views,
              AVG(page_load_time_ms) as avg_pg_ld,
              sum(page_load_time_ms) as total_pg_ld,
              PERCENTILE(
                page_load_time_ms,
                array(0.05,0.10,0.15,0.20,0.25,0.30,0.35,0.40,0.45,0.5,0.55,0.60,0.65,0.70,0.75,0.80,0.85,0.90,0.95,0.99)) AS a
      FROM ( -- dictionary
            SELECT partition_date as partition_date_denver,
                   prod.epoch_datehour(cast(message__timestamp * 1000 as bigint),'America/Denver') as partition_date_hour_denver,
                   visit__settings['post_prop73'] as browser_size_breakpoint,
                   state__view__current_page__name AS page_name,
                   state__view__current_page__page_id AS page_id,
                   state__view__current_page__page_load_time AS page_load_time_ms,
                   visit__device__browser__name as browser_name,
                   visit__visit_id AS visit_id,
                   case WHEN state__view__current_page__name = 'billing-and-transactions'
                              THEN 'Billing'
                        WHEN state__view__current_page__name = 'my-tv-services'
                              THEN 'Equipment - TV'
                        WHEN state__view__current_page__name = 'my-internet-services'
                              THEN 'Equipment - Internet'
                        WHEN state__view__current_page__name = 'my-voice-services'
                              THEN 'Equipment - Voice'
                        WHEN state__view__current_page__name = 'home-authenticated'
                              THEN 'Home - Authenticated'
                        WHEN state__view__current_page__name = 'home-unauth'
                              THEN 'Home - Unauthenticated'
                        ELSE 'Unspecified'
                   END AS section
            FROM asp_v_net_events
            WHERE ( partition_date >= DATE_SUB("${env:RUN_DATE}",${hiveconf:ProcessingLag}))
                AND message__category = 'Page View'
                AND state__view__current_page__page_load_time > 0
                AND state__view__current_page__page_load_time < 600000 -- 10 mins
                AND state__view__current_page__name In(   'my-tv-services',
                                                          'my-internet-services',
                                                          'my-voice-services',
                                                          'home-authenticated',
                                                          'home-unauth',
                                                          'billing-and-transactions')
           ) dictionary
      GROUP BY
                partition_date_denver,
                partition_date_hour_denver,
                browser_name,
                browser_size_breakpoint,
                page_name,
                section
      GROUPING SETS ( (page_name),
                      (browser_name, page_name),
                      (browser_name, section, page_name),
                      (partition_date_denver, browser_name, page_name),
                      (partition_date_denver, browser_name, browser_size_breakpoint, page_name),
                      (partition_date_denver, section, browser_size_breakpoint, page_name),
                      (partition_date_denver, page_name),
                      (partition_date_denver, partition_date_hour_denver, page_name),
                      (partition_date_denver, partition_date_hour_denver, browser_name, page_name)
                    ) -- GROUPING SETS
) p_tiles
;

INSERT OVERWRITE TABLE asp_agg_page_load_time_grouping_sets PARTITION(partition_date_denver)
select  grouping_id,
        grouped_partition_date_denver,
        grouped_partition_date_hour_denver,
        grouped_browser_name,
        grouped_breakpoints,
        partition_date_hour_denver,
        browser_name,
        browser_size_breakpoint,
        page_name,
        section,
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
  FROM ${env:TMP_db}.asp_agg_page_load_time_grouping_sets;
