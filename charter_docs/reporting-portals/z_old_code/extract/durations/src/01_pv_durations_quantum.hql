USE ${env:ENVIRONMENT}
;



------------------------------------------------------------

SELECT '***** Preparing stat tables for use ******'
;

DROP TABLE IF EXISTS duration_stats_avg_quantum;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_avg_quantum (
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

DROP TABLE IF EXISTS duration_stats_05_quantum;
DROP TABLE IF EXISTS duration_stats_25_quantum;
DROP TABLE IF EXISTS duration_stats_50_quantum;
DROP TABLE IF EXISTS duration_stats_75_quantum;
DROP TABLE IF EXISTS duration_stats_95_quantum;
DROP TABLE IF EXISTS duration_stats_stdev_quantum;
DROP TABLE IF EXISTS duration_stats_min_quantum;
DROP TABLE IF EXISTS duration_stats_max_quantum;
DROP TABLE IF EXISTS duration_stats_total_quantum;
DROP TABLE IF EXISTS duration_stats_count_quantum;

CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_05_quantum like duration_stats_avg_quantum;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_25_quantum like duration_stats_avg_quantum;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_50_quantum like duration_stats_avg_quantum;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_75_quantum like duration_stats_avg_quantum;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_95_quantum like duration_stats_avg_quantum;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_stdev_quantum like duration_stats_avg_quantum;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_min_quantum like duration_stats_avg_quantum;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_max_quantum like duration_stats_avg_quantum;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_total_quantum like duration_stats_avg_quantum;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_count_quantum like duration_stats_avg_quantum;

SELECT '***** Calculating Stats tables for steps ******'
;

FROM
  (
SELECT
      DOMAIN,
      current_page,
      next_page,
      avg_visit_duration/1000 AS avg_visit_duration_sec,
      percentiles_visit_duration[0]/1000 AS perc_visit_duration_sec_5th,
      percentiles_visit_duration[1]/1000 AS perc_visit_duration_sec_25th,
      percentiles_visit_duration[2]/1000 AS perc_visit_duration_sec_median,
      percentiles_visit_duration[3]/1000 AS perc_visit_duration_sec_75th,
      percentiles_visit_duration[4]/1000 AS perc_visit_duration_sec_95th,
      stdev_visit_duration/1000 AS stdev_visit_duration_sec,
      min_visit_duration/1000 AS min_visit_duration_sec,
      max_visit_duration/1000 AS max_visit_duration_sec,
      total_visit_duration/1000 AS total_visit_duration_sec,
      total_page_views,
      denver_Date AS partition_date,
    '${env:source}' AS source_table
FROM (
SELECT denver_Date,
       DOMAIN,
       current_page,
       next_page,
       --calculate metrics based on current_page and next page

       AVG(current_page_duration) AS avg_visit_duration,
       PERCENTILE(current_page_duration, array(0.05, 0.25, 0.5, 0.75, 0.95)) AS percentiles_visit_duration,
       stddev_pop(current_page_duration) AS stdev_visit_duration,
       MIN(current_page_duration) AS min_visit_duration,
       MAX(current_page_duration) AS max_visit_duration,
       SUM(current_page_duration) AS total_visit_duration,
       sum(page_view_cnts) AS total_page_views
FROM
  (SELECT epoch_converter(received__timestamp,'America/Denver') AS denver_Date,
          CASE
              WHEN lower(visit__application_details__application_name)='specnet' THEN 'resi'
              WHEN lower(visit__application_details__application_name)='myspectrum' THEN 'app'
              WHEN lower(visit__application_details__application_name)='smb' THEN 'sb'
          END AS DOMAIN,
          visit__visit_id,
          --current page mapped to next page to calulate the duration captured

          state__view__current_page__page_name AS next_page,
          --we have duration calculated for previous page within the platform and treating previous page as current page

          state__view__previous_page__page_name AS current_page,
          max(state__view__previous_page__page_viewed_time_ms) AS current_page_duration,
          --number of page visits in a session(visit)

          count(*) AS page_view_cnts
   FROM ${env:source}
WHERE (${env:date_field} >= '${env:start}' AND ${env:date_field} < '${env:end}')
     AND state__view__previous_page__page_name IS NOT NULL
     AND message__name ='pageView'
   GROUP BY epoch_converter(received__timestamp,'America/Denver'),
            CASE
                WHEN lower(visit__application_details__application_name)='specnet' THEN 'resi'
                WHEN lower(visit__application_details__application_name)='myspectrum' THEN 'app'
                WHEN lower(visit__application_details__application_name)='smb' THEN 'sb'
            END,
            visit__visit_id,
            state__view__current_page__page_name,
            state__view__previous_page__page_name) perc_calc
GROUP BY denver_Date,
         DOMAIN,
         current_page,
         next_page) final_durations
        -- other dimensions
  ) stats_prep
  INSERT OVERWRITE TABLE duration_stats_avg_quantum
    SELECT
      current_page,
      next_page, --next_step,
      avg_visit_duration_sec,
      partition_date,
      'Average' as metric,-- metric,
      'Page Views' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_05_quantum
    SELECT
      current_page,
      next_page, --next_step,
      perc_visit_duration_sec_5th,
      partition_date,
      '.05 percentile' as metric,-- metric,
      'Page Views' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_25_quantum
    SELECT
      current_page,
      next_page, --next_step,
      perc_visit_duration_sec_25th,
      partition_date,
      '.25 percentile' as metric,-- metric,
      'Page Views' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_50_quantum
    SELECT
      current_page,
      next_page, --next_step,
      perc_visit_duration_sec_median,
      partition_date,
      '.5 percentile' as metric,-- metric,
      'Page Views' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_75_quantum
    SELECT
      current_page,
      next_page, --next_step,
      perc_visit_duration_sec_75th,
      partition_date,
      '.75 percentile' as metric,-- metric,
      'Page Views' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_95_quantum
    SELECT
      current_page,
      next_page, --next_step,
      perc_visit_duration_sec_95th,
      partition_date,
      '.95 percentile' as metric,-- metric,
      'Page Views' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_stdev_quantum
    SELECT
      current_page,
      next_page, --next_step,
      stdev_visit_duration_sec,
      partition_date,
      'stdev' as metric,-- metric,
      'Page Views' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_min_quantum
    SELECT
      current_page,
      next_page, --next_step,
      min_visit_duration_sec,
      partition_date,
      'min' as metric,-- metric,
      'Page Views' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_max_quantum
    SELECT
      current_page,
      next_page, --next_step,
      max_visit_duration_sec,
      partition_date,
      'max' as metric,-- metric,
      'Page Views' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_total_quantum
    SELECT
      current_page,
      next_page, --next_step,
      total_visit_duration_sec,
      partition_date,
      'total' as metric,-- metric,
      'Page Views' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_count_quantum
    SELECT
      current_page,
      next_page, --next_step,
      total_page_views,
      partition_date,
      'count' as metric,-- metric,
      'Page Views' as duration_type,
      domain,
      source_table
;

INSERT OVERWRITE TABLE asp_duration_stats_quantum PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_avg_quantum
;
INSERT OVERWRITE TABLE asp_duration_stats_quantum PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_05_quantum
;
INSERT OVERWRITE TABLE asp_duration_stats_quantum PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_25_quantum
;
INSERT OVERWRITE TABLE asp_duration_stats_quantum PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_50_quantum
;
INSERT OVERWRITE TABLE asp_duration_stats_quantum PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_75_quantum
;
INSERT OVERWRITE TABLE asp_duration_stats_quantum PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_95_quantum
;
INSERT OVERWRITE TABLE asp_duration_stats_quantum PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_stdev_quantum
;
INSERT OVERWRITE TABLE asp_duration_stats_quantum PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_min_quantum
;
INSERT OVERWRITE TABLE asp_duration_stats_quantum PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_max_quantum
;
INSERT OVERWRITE TABLE asp_duration_stats_quantum PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_total_quantum
;
INSERT OVERWRITE TABLE asp_duration_stats_quantum PARTITION (partition_date, metric, duration_type, domain, source_table)
  SELECT * FROM duration_stats_count_quantum
;

SELECT '***** END STEP FOUR ******'
;
