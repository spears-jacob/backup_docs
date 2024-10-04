use ${env:ENVIRONMENT};

SELECT "############################################################################################################################### ";
SELECT "### Running load_specguide_wb_metrics.hql for Daily metrics for load date: '${hiveconf:LOAD_DATE}'  ";
 -- This job is for daily metrics processing...
 -- If a change is made in the below query, ensure the same change is made in the monthly metrics code as well
 -- This pulls metrics from the specguide wb netflix events table


SELECT "### Add Performance Tuning Hints";
SET hive.stats.fetch.column.stats=false;

SELECT "### Truncate ${env:TMP_db}.specguide_worldbox_metrics before loading new metrics";

TRUNCATE TABLE ${env:TMP_db}.specguide_worldbox_metrics;

SELECT "### Loading Daily metrics for WorldBox Netflix usage for '${hiveconf:LOAD_DATE}' ";
-- INSERT OVERWRITE TABLE ${env:TMP_db}.specguide_worldbox_metric_array
WITH wb_data as
(
      SELECT
            base_q.account_type,
            CAST(grouping__id AS int) AS grouping_id,
            Map(
            'Number of Visits', sum(base_q.nbr_visits),
            'Number of Upgrades', sum(base_q.nbr_upgrades),
            'Number of Launches', sum(base_q.nbr_launches),
            'Number of Devices with Visits', sum(base_q.nbr_devices_with_visit),
            'Number of Households with Visits', sum(base_q.nbr_hhs_with_visit),
            'Number of Devices with Upgrades', sum(base_q.nbr_devices_with_upgrade),
            'Number of Households with Upgrades', sum(base_q.nbr_hhs_with_upgrade),
            'Number of Devices with Launches', sum(base_q.nbr_devices_with_launch),
            'Number of Households with Launches', sum(base_q.nbr_hhs_with_launch),
            'Number of Devices', sum(base_q.nbr_devices),
            'Number of Households', sum(base_q.nbr_hhs)
              ) AS tmp_map
      FROM (
                  SELECT
                        act.account__type AS account_type,
                        size(collect_set(events.visit__visit_id)) AS nbr_visits,
                        size(collect_set(events.upgrade_visit_id)) AS nbr_upgrades,
                        size(collect_set(events.launch_visit_id)) AS nbr_launches,
                        size(collect_set(IF(events.visit__visit_id IS NOT NULL, events.account__mac_id_aes256_standard, null))) AS nbr_devices_with_visit,
                        size(collect_set(IF(events.visit__visit_id IS NOT NULL, events.account__number_aes256, null))) AS nbr_hhs_with_visit,
                        size(collect_set(IF(events.upgrade_visit_id IS NOT NULL, events.account__mac_id_aes256_standard, null))) AS nbr_devices_with_upgrade,
                        size(collect_set(IF(events.upgrade_visit_id IS NOT NULL, events.account__number_aes256, null))) AS nbr_hhs_with_upgrade,
                        size(collect_set(IF(events.launch_visit_id IS NOT NULL, events.account__mac_id_aes256_standard, null))) AS nbr_devices_with_launch,
                        size(collect_set(IF(events.launch_visit_id IS NOT NULL, events.account__number_aes256, null))) AS nbr_hhs_with_launch,
                        size(collect_set(events.account__mac_id_aes256_standard)) AS nbr_devices,
                        size(collect_set(events.account__number_aes256)) AS nbr_hhs
                  FROM ${env:ENVIRONMENT}.specguide_wb_netflix_events events
                  LEFT JOIN ${env:ENVIRONMENT}.specguide_worldbox_activity act
                  ON act.partition_date_denver = events.partition_date_denver
                  AND act.account__mac_id_aes256_standard = events.account__mac_id_aes256_standard
                  WHERE (events.partition_date_denver = '${hiveconf:LOAD_DATE}')
                  GROUP BY
                        act.account__type
          ) base_q
      GROUP BY
            base_q.account_type
            GROUPING SETS
            (
                  (),
                  (base_q.account_type)
            )
)

INSERT INTO ${env:TMP_db}.specguide_worldbox_metrics
  (
  partition_date_denver,
  analysis_period,
  analysis_period_end_date,
  account_type,
  metric_data_source,
  grouping_id,
  metric_name,
  metric_value
  )
SELECT
  '${hiveconf:LOAD_DATE}' AS partition_date_denver,
  'Daily' AS analysis_period,
  '${hiveconf:LOAD_DATE}' AS analysis_period_end_date,
  CASE WHEN (wb_data.grouping_id & 1) != 0 THEN wb_data.account_type ELSE 'All Account Types' END AS account_type,
  'Venona Events' AS metric_data_source,
  wb_data.grouping_id,
  metric_name,
  metric_value
FROM
  wb_data
  LATERAL VIEW EXPLODE(wb_data.tmp_map) explode_table AS metric_name, metric_value
;



-- specguide worldbox metrics table has daily and monthly results - before insert/overwrite, copy the metrics (if any)
-- from other analysis periods to the temp table before overwriting the partition
SELECT "### Copy existing data from specguide_worldbox_metrics for other analysis periods before replacing. ";

INSERT INTO TABLE ${env:TMP_db}.specguide_worldbox_metrics
SELECT
    partition_date_denver,
    analysis_period,
    analysis_period_end_date,
    account_type,
    metric_data_source,
    grouping_id,
    metric_name,
    metric_value
FROM ${env:ENVIRONMENT}.specguide_worldbox_metrics
WHERE partition_date_denver = '${hiveconf:LOAD_DATE}'
AND analysis_period != 'Daily'
;


SELECT "### Overwrite specguide_worldbox_metrics from temp table for partition date: '${hiveconf:LOAD_DATE}' ";

INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.specguide_worldbox_metrics PARTITION (partition_date_denver = '${hiveconf:LOAD_DATE}')
SELECT
    analysis_period,
    analysis_period_end_date,
    account_type,
    metric_data_source,
    grouping_id,
    metric_name,
    metric_value,
    current_date() AS date_loaded
FROM ${env:TMP_db}.specguide_worldbox_metrics
WHERE partition_date_denver = '${hiveconf:LOAD_DATE}'
;


SELECT "### Done Running load_specguide_wb_metrics_daily.hql for load date: '${hiveconf:LOAD_DATE}' ";
