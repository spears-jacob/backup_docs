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


------------------------------------------------------------

SELECT '***** Preparing stat tables for use ******'
;

DROP TABLE IF EXISTS duration_stats_avg_quantum;
CREATE TEMPORARY TABLE IF NOT EXISTS duration_stats_avg_quantum (
  current_page            string,
  next_page               string,
  value                   string,
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


SELECT '***** Calculating Stats tables for visit durations ******'
;

FROM
  (
      SELECT
      'Visit_Duration_Start' As current_page,
      'Visit_Duration_End' AS next_page,
       DOMAIN,
       avg_visit_duration/1000 AS avg_visit_duration,
       percentiles_visit_duration[0]/1000 as perc_visit_duration_sec_5th,
       percentiles_visit_duration[1]/1000 as perc_visit_duration__sec25th,
       percentiles_visit_duration[2]/1000 as perc_visit_duration_sec_median,
       percentiles_visit_duration[3]/1000 as perc_visit_duration_sec_75th,
       percentiles_visit_duration[4]/1000 as perc_visit_duration_sec_95th,
       stdev_visit_duration/1000 as stdev_visit_duration_sec,
       min_visit_duration/1000 as min_visit_duration_sec,
       max_visit_duration/1000 as max_visit_duration_sec,
       total_visit_duration/1000 as total_visit_duration_sec,
       visits_count,
       denver_Date as partition_Date,
       '${env:source}' as source_table
FROM
  (SELECT denver_Date,
          DOMAIN,
          --calculate aggregates based the duration based on app_type and denver_date
          AVG(duration) AS avg_visit_duration,
          PERCENTILE(duration, array(0.05, 0.25, 0.5, 0.75, 0.95)) AS percentiles_visit_duration,
          stddev_pop(duration) AS stdev_visit_duration,
          MIN(duration) AS min_visit_duration,
          MAX(duration) AS max_visit_duration,
          SUM(duration) AS total_visit_duration,
          count(*) AS visits_count
   FROM
     (SELECT epoch_converter(received__timestamp,'America/Denver') AS denver_Date,
             CASE WHEN lower(visit__application_details__application_name)='specnet' THEN 'Resi'
             WHEN lower(visit__application_details__application_name)='myspectrum' THEN 'app'
             WHEN lower(visit__application_details__application_name)='smb' THEN 'sb' END AS DOMAIN,
visit__visit_id,
--calculate the duration of a visit based on min and max timestamp
max(received__timestamp)-min(received__timestamp) AS duration
      FROM ${env:source}
WHERE (${env:date_field} >= '${env:start}' AND ${env:date_field} < '${env:end}')
      GROUP BY epoch_converter(received__timestamp,'America/Denver'),
               CASE WHEN lower(visit__application_details__application_name)='specnet' THEN 'resi' 
               WHEN lower(visit__application_details__application_name)='myspectrum' THEN 'app'
               WHEN lower(visit__application_details__application_name)='smb' THEN 'sb' END,
visit__visit_id) perc_calc
   GROUP BY denver_Date,
          DOMAIN) final_durations

  ) stats_prep
  INSERT OVERWRITE TABLE duration_stats_avg_quantum
    SELECT
      current_page,
      next_page, --next_step,
      avg_visit_duration,
      partition_date,
      'Average' as metric,-- metric,
      'Site Visit' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_05_quantum
    SELECT
      current_page,
      next_page, --next_step,
      perc_visit_duration_sec_5th,
      partition_date,
      '.05 percentile' as metric,-- metric,
      'Site Visit' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_25_quantum
    SELECT
      current_page,
      next_page, --next_step,
      perc_visit_duration__sec25th,
      partition_date,
      '.25 percentile' as metric,-- metric,
      'Site Visit' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_50_quantum
    SELECT
      current_page,
      next_page, --next_step,
      perc_visit_duration_sec_median,
      partition_date,
      '.5 percentile' as metric,-- metric,
      'Site Visit' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_75_quantum
    SELECT
      current_page,
      next_page, --next_step,
      perc_visit_duration_sec_75th,
      partition_date,
      '.75 percentile' as metric,-- metric,
      'Site Visit' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_95_quantum
    SELECT
      current_page,
      next_page, --next_step,
      perc_visit_duration_sec_95th,
      partition_date,
      '.95 percentile' as metric,-- metric,
      'Site Visit' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_stdev_quantum
    SELECT
      current_page,
      next_page, --next_step,
      stdev_visit_duration_sec,
      partition_date,
      'stdev' as metric,-- metric,
      'Site Visit' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_min_quantum
    SELECT
      current_page,
      next_page, --next_step,
      min_visit_duration_sec,
      partition_date,
      'min' as metric,-- metric,
      'Site Visit' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_max_quantum
    SELECT
      current_page,
      next_page, --next_step,
      max_visit_duration_sec,
      partition_date,
      'max' as metric,-- metric,
      'Site Visit' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_total_quantum
    SELECT
      current_page,
      next_page, --next_step,
      total_visit_duration_sec,
      partition_date,
      'total' as metric,-- metric,
      'Site Visit' as duration_type,
      domain,
      source_table
  INSERT OVERWRITE TABLE duration_stats_count_quantum
    SELECT
      current_page,
      next_page, --next_step,
      visits_count,
      partition_date,
      'count' as metric,-- metric,
      'Site Visit' as duration_type,
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
