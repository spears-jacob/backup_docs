-----------------------------------------------------------------------------------------------------
-- The below query is to understand the total duration per visit on the residential spectrum website
-- Query is not split out by pages and does not distinguish between page views or custom link clicks
-- Query only considers first and last time stamp in a visit
-----------------------------------------------------------------------------------------------------

USE ${env:ENVIRONMENT}
;

------------------------------------------------------------
--STEP ONE:
--Get first and last timestamp per visit ID and calculate difference.
------------------------------------------------------------

CREATE TEMPORARY TABLE site_durations_cj AS
      --Subselect calculates the difference between the first and last timestamp for a total duration on the site from each visit id
    select
      partition_date,
      visit__visit_id,
      end_time - start_time AS duration
      from
        (
          --Subselect takes the first and last timestamp from each visit id
          SELECT
            ${env:date_field} AS partition_date,
            visit__visit_id,
            min(message__timestamp)
              OVER (PARTITION BY visit__visit_id) AS start_time,
            max(message__timestamp)
              OVER (PARTITION BY visit__visit_id) AS end_time

          FROM ${env:source}
          WHERE
            (${env:date_field} >= '${env:start}' AND ${env:date_field} < '${env:end}')
          ) a

    group by
      partition_date,
      visit__visit_id,
      start_time,
      end_time
;

------------------------------------------------------------

SELECT '***** Preparing stat tables for use ******'
;

DROP TABLE IF EXISTS duration_stats_avg;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_avg (
  current_page            string,
  next_page               string,
  value            decimal(18,3),
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


SELECT '***** Calculating Stats tables for visit durations ******'
;

FROM
  (
      SELECT
        partition_date,
        'Visit_Duration_Start' As current_page,
        'Visit_Duration_End' AS next_page,
        AVG(duration) AS avg_visit_duration,
        PERCENTILE(duration, array(0.05, 0.25, 0.5, 0.75, 0.95)) AS percentiles_visit_duration,
        stddev_pop(duration) AS stdev_visit_duration,
        MIN(duration) AS min_visit_duration,
        MAX(duration) AS max_visit_duration,
        SUM(duration) AS total_visit_duration,
        count(*) AS count_visit_duration,
        '${env:source}' AS source_table

      FROM site_durations_cj
      GROUP BY
        partition_date

  ) stats_prep
  INSERT OVERWRITE TABLE duration_stats_avg
    SELECT
      current_page,
      next_page, --next_step,
      avg_visit_duration,
      partition_date,
      'Average' as metric,-- metric,
      'Site Visit' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_05
    SELECT
      current_page,
      next_page, --next_step,
      percentiles_visit_duration[0],
      partition_date,
      '.05 percentile' as metric,-- metric,
      'Site Visit' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_25
    SELECT
      current_page,
      next_page, --next_step,
      percentiles_visit_duration[1],
      partition_date,
      '.25 percentile' as metric,-- metric,
      'Site Visit' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_50
    SELECT
      current_page,
      next_page, --next_step,
      percentiles_visit_duration[2],
      partition_date,
      '.5 percentile' as metric,-- metric,
      'Site Visit' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_75
    SELECT
      current_page,
      next_page, --next_step,
      percentiles_visit_duration[3],
      partition_date,
      '.75 percentile' as metric,-- metric,
      'Site Visit' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_95
    SELECT
      current_page,
      next_page, --next_step,
      percentiles_visit_duration[4],
      partition_date,
      '.95 percentile' as metric,-- metric,
      'Site Visit' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_stdev
    SELECT
      current_page,
      next_page, --next_step,
      stdev_visit_duration,
      partition_date,
      'stdev' as metric,-- metric,
      'Site Visit' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_min
    SELECT
      current_page,
      next_page, --next_step,
      min_visit_duration,
      partition_date,
      'min' as metric,-- metric,
      'Site Visit' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_max
    SELECT
      current_page,
      next_page, --next_step,
      max_visit_duration,
      partition_date,
      'max' as metric,-- metric,
      'Site Visit' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_total
    SELECT
      current_page,
      next_page, --next_step,
      total_visit_duration,
      partition_date,
      'total' as metric,-- metric,
      'Site Visit' as duration_type,
      '${env:domain}' AS domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_count
    SELECT
      current_page,
      next_page, --next_step,
      count_visit_duration,
      partition_date,
      'count' as metric,-- metric,
      'Site Visit' as duration_type,
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
