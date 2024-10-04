USE ${env:ENVIRONMENT}
;

------------------------------------------------------------
--STEP ONE:
--Populate raw duration table with all page views from net_events
------------------------------------------------------------
SELECT '***** BEGIN STEP ONE: asp_metrics_duration_raw ******'
;

-- All Page Views
-- no grouping
INSERT OVERWRITE TABLE asp_pvs PARTITION (partition_date, source_table)
SELECT
  visit__visit_id,
  ${env:page} AS state__view__current_page__name,
  message__timestamp,
  message__category,
  ${env:date_field} AS partition_date,
  '${env:source}' AS source_table
FROM ${env:source}
WHERE
    (${env:date_field} >= '${env:start}' AND ${env:date_field} < '${env:end}')
  AND message__category = 'Page View'
;

SELECT '***** END PART ONE: Populate with production suite data ******'
;


------------------------------------------------------------
--asp_pv_pairs
--STEP TWO:
--Populate raw lag and lead values for page view order
------------------------------------------------------------
SELECT '***** BEGIN STEP TWO:  asp_pv_pairs ******'
;

-- This table has all the PVs still (#noFilter)
INSERT OVERWRITE TABLE asp_pv_pairs PARTITION (partition_date, source_table)
SELECT
  visit__visit_id,
  state__view__current_page__name,
  message__category,
  message__timestamp,
  lead(state__view__current_page__name)
    OVER (PARTITION BY visit__visit_id ORDER BY message__timestamp) AS next_page,
  lead(message__timestamp)
    OVER (PARTITION BY visit__visit_id ORDER BY message__timestamp) AS next_timestamp,
  partition_date,
  source_table
FROM asp_pvs
WHERE
    (partition_date >= '${env:start}' AND partition_date < '${env:end}'
    AND '${env:source}' = source_table)

GROUP BY
  partition_date,
  visit__visit_id,
  message__category,
  state__view__current_page__name,
  message__timestamp,
  source_table
;

SELECT '***** END STEP TWO: asp_pv_pairs ******'
;

------------------------------------------------------------
--asp_pv_duration
--STEP THREE:
-- Page View Duration table takes all pages and figures out the next page and the time taken between them
------------------------------------------------------------
SELECT '***** BEGIN STEP THREE: asp_pv_duration ******'
;

INSERT OVERWRITE TABLE asp_pv_duration PARTITION (partition_date,source_table)
SELECT
  visit__visit_id,
  state__view__current_page__name as current_page,
  next_page as next_page,
  next_timestamp - message__timestamp AS duration,  -- get the amount of time spent on a page by subtracting the start time of their next page from the current page start time
  partition_date,
  source_table
FROM asp_pv_pairs
  WHERE
  (partition_date >= '${env:start}' AND partition_date < '${env:end}'
  AND '${env:source}' = source_table)
;

SELECT '***** END STEP THREE: asp_pv_duration ******'
;

------------------------------------------------------------

SELECT '***** Preparing stat tables for use ******'
;

DROP TABLE IF EXISTS duration_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_avg (
  current_page            string,
  next_page               string,
  value                   decimal(18,3),
  partition_date          string,
  metric                  string,
  duration_type           string,
  domain                  string,
  source_table            string
)
;

DROP TABLE IF EXISTS duration_stats_05;
DROP TABLE IF EXISTS duration_stats_25;
DROP TABLE IF EXISTS duration_stats_50;
DROP TABLE IF EXISTS duration_stats_75;
DROP TABLE IF EXISTS duration_stats_95;
DROP TABLE IF EXISTS duration_stats_stdev;
DROP TABLE IF EXISTS duration_stats_min;
DROP TABLE IF EXISTS duration_stats_max;
DROP TABLE IF EXISTS duration_stats_total;
DROP TABLE IF EXISTS duration_stats_count;

CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_05 like duration_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_25 like duration_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_50 like duration_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_75 like duration_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_95 like duration_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_stdev like duration_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_min like duration_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_max like duration_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_total like duration_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_count like duration_stats_avg;

SELECT '***** Calculating Stats tables for steps ******'
;

FROM
  (
      SELECT
        partition_date,
        current_page,
        next_page,
        AVG(duration) AS avg_to_next_step,
        PERCENTILE(duration, array(0.05, 0.25, 0.5, 0.75, 0.95)) AS percentiles_to_next_step,
        stddev_pop(duration) AS stdev_to_next_step,
        MIN(duration) AS min_to_next_step,
        MAX(duration) AS max_to_next_step,
        SUM(duration) AS total_to_next_step,
        count(*) AS count_to_next_step,
        source_table
      FROM asp_pv_duration
      WHERE
      (partition_date >= '${env:start}' AND partition_date < '${env:end}'
        AND '${env:source}' = source_table)
      GROUP BY
        partition_date,
        current_page,
        next_page,
        source_table
        -- other dimensions
  ) stats_prep
  INSERT OVERWRITE TABLE duration_stats_avg
    SELECT
      current_page,
      next_page, --next_step,
      avg_to_next_step,
      partition_date,
      'Average' as metric,-- metric,
      'Page Views' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_05
    SELECT
      current_page,
      next_page, --next_step,
      percentiles_to_next_step[0],
      partition_date,
      '.05 percentile' as metric,-- metric,
      'Page Views' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_25
    SELECT
      current_page,
      next_page, --next_step,
      percentiles_to_next_step[1],
      partition_date,
      '.25 percentile' as metric,-- metric,
      'Page Views' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_50
    SELECT
      current_page,
      next_page, --next_step,
      percentiles_to_next_step[2],
      partition_date,
      '.5 percentile' as metric,-- metric,
      'Page Views' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_75
    SELECT
      current_page,
      next_page, --next_step,
      percentiles_to_next_step[3],
      partition_date,
      '.75 percentile' as metric,-- metric,
      'Page Views' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_95
    SELECT
      current_page,
      next_page, --next_step,
      percentiles_to_next_step[4],
      partition_date,
      '.95 percentile' as metric,-- metric,
      'Page Views' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_stdev
    SELECT
      current_page,
      next_page, --next_step,
      stdev_to_next_step,
      partition_date,
      'stdev' as metric,-- metric,
      'Page Views' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_min
    SELECT
      current_page,
      next_page, --next_step,
      min_to_next_step,
      partition_date,
      'min' as metric,-- metric,
      'Page Views' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_max
    SELECT
      current_page,
      next_page, --next_step,
      max_to_next_step,
      partition_date,
      'max' as metric,-- metric,
      'Page Views' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_total
    SELECT
      current_page,
      next_page, --next_step,
      total_to_next_step,
      partition_date,
      'total' as metric,-- metric,
      'Page Views' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_count
    SELECT
      current_page,
      next_page, --next_step,
      count_to_next_step,
      partition_date,
      'count' as metric,-- metric,
      'Page Views' as duration_type,
      '${env:domain}' AS domain,
      source_table
;

INSERT OVERWRITE TABLE asp_duration_stats PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_avg
;
INSERT OVERWRITE TABLE asp_duration_stats PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_05
;
INSERT OVERWRITE TABLE asp_duration_stats PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_25
;
INSERT OVERWRITE TABLE asp_duration_stats PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_50
;
INSERT OVERWRITE TABLE asp_duration_stats PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_75
;
INSERT OVERWRITE TABLE asp_duration_stats PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_95
;
INSERT OVERWRITE TABLE asp_duration_stats PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_stdev
;
INSERT OVERWRITE TABLE asp_duration_stats PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_min
;
INSERT OVERWRITE TABLE asp_duration_stats PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_max
;
INSERT OVERWRITE TABLE asp_duration_stats PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_total
;
INSERT OVERWRITE TABLE asp_duration_stats PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_count
;

SELECT '***** END STEP FOUR ******'
;
