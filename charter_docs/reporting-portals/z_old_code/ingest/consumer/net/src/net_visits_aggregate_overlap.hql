use ${env:ENVIRONMENT};

SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.exec.parallel=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.auto.convert.join=false;

SET ONE_HOUR_TIMESTAMP=3600;
SET ONE_DAY_TIMESTAMP=86400;
SET TWO_DAYS_TIMESTAMP=172800;
SET ONE_WEEK_TIMESTAMP=604800;
SET ONE_MONTH_TIMESTAMP=2592000;
SET ONE_QUARTER_TIMESTAMP=7776000;
SET ONE_HALFYEAR_TIMESTAMP=15552000;
SET ONE_YEAR_TIMESTAMP=31104000;
SET THREE_AM_TIMESTAMP_DIFF=10800;
SET TWELVE_PM_TIMESTAMP_DIFF=43200;
SET SIX_PM_TIMESTAMP_DIFF=64800;
SET NUMBER_OF_DAYS=1;

DROP TABLE IF EXISTS ${env:TMP_db}.net_events_daily;
CREATE TABLE ${env:TMP_db}.net_events_daily LIKE net_events;
INSERT INTO TABLE ${env:TMP_db}.net_events_daily PARTITION(partition_date)
  SELECT -- grab all net_event fields for correct partition_dates that match existing net session ids
    net_events.*
  FROM
    net_events
  INNER JOIN
    ${env:TMP_db}.net_events_existing ON (net_events.visit__visit_id = net_events_existing.visit_id)
  WHERE
  CAST(net_events.partition_date AS DATE) BETWEEN CAST('${hiveconf:LAST_DATE}' AS DATE) AND DATE_ADD(CAST('${hiveconf:LAST_DATE}' AS DATE), ${hiveconf:NUMBER_OF_DAYS});

SET hive.execution.engine=mr;

DROP TABLE IF EXISTS ${env:TMP_db}.visitor_net;
CREATE TABLE ${env:TMP_db}.visitor_net
(
  visitor_id STRING,
  total_visits_last_day INT,
  total_visits_last_week INT,
  total_visits_last_month INT,
  total_visits_last_3_months INT,
  total_visits_last_6_months INT,
  total_visits_last_year INT
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as orc;

INSERT INTO TABLE ${env:TMP_db}.visitor_net
SELECT
   visitor_id,
   SUM(IF(total_visits_last_day > 0, 1, 0)) AS total_visits_last_day,
   SUM(IF(total_visits_last_week > 0, 1, 0)) AS total_visits_last_week,
   SUM(IF(total_visits_last_month > 0, 1, 0)) AS total_visits_last_month,
   SUM(IF(total_visits_last_3_months > 0, 1, 0)) AS total_visits_last_3_months,
   SUM(IF(total_visits_last_6_months > 0, 1, 0)) AS total_visits_last_6_months,
   SUM(IF(total_visits_last_year > 0, 1, 0)) AS total_visits_last_year
FROM (
  SELECT
    visitor_id AS visitor_id,
    visit_id AS visit_id,
    SUM(IF(start_timestamp >= unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}'), 1, 0)) AS total_visits_last_day,
    SUM(IF(start_timestamp >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_WEEK_TIMESTAMP}), 1, 0)) AS total_visits_last_week,
    SUM(IF(start_timestamp >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_MONTH_TIMESTAMP}), 1, 0)) AS total_visits_last_month,
    SUM(IF(start_timestamp >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_QUARTER_TIMESTAMP}), 1, 0)) AS total_visits_last_3_months,
    SUM(IF(start_timestamp >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_HALFYEAR_TIMESTAMP}), 1, 0)) AS total_visits_last_6_months,
    SUM(IF(start_timestamp >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_YEAR_TIMESTAMP}), 1, 0)) AS total_visits_last_year
  FROM
    net_visits_agg
  WHERE
    visitor_id IS NOT NULL AND
    visitor_id <> "" AND
    start_timestamp IS NOT NULL AND
    start_timestamp >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_YEAR_TIMESTAMP}) AND 
    start_timestamp <= unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') AND 
    net_visits_agg.visit_id IN (SELECT visit_id FROM ${env:TMP_db}.net_events_existing)
  GROUP BY visitor_id, visit_id
) AS visitor_net_temp
GROUP BY visitor_id;

SET hive.execution.engine=tez;

DROP TABLE IF EXISTS ${env:TMP_db}.net_visits_agg;
CREATE TABLE ${env:TMP_db}.net_visits_agg LIKE net_visits_agg;
INSERT INTO TABLE ${env:TMP_db}.net_visits_agg PARTITION(year) SELECT * FROM net_visits_agg WHERE year = '${hiveconf:LAST_YEAR}' AND NOT EXISTS (SELECT net_events_existing.visit_id FROM ${env:TMP_db}.net_events_existing WHERE net_visits_agg.visit_id = net_events_existing.visit_id);

ALTER TABLE net_visits_agg DROP IF EXISTS PARTITION (year='${hiveconf:LAST_YEAR}');
INSERT INTO TABLE net_visits_agg PARTITION(year) SELECT * FROM ${env:TMP_db}.net_visits_agg;

SET hive.execution.engine=mr;

INSERT INTO TABLE net_visits_agg PARTITION(year)
SELECT
  visit__visit_id AS visit_id,
  MAX(visit__device__enc_uuid) AS visitor_id,
  MAX(visit__connection__type) AS network_type,
  named_struct
  (
    "user_id", MAX(visit__user__enc_id),
    "account_id", MAX(visit__account__enc_account_number),
    "test_account", MAX(visit__user__is_test_user),
    "headend", "",
    "network_status",collect_set(visit__connection__network_status),
    "subscriptionType", MAX(visit__account__subscription__service_level)
  ) AS customer,
  named_struct
  (
    "type", "",
    "model", MAX(visit__device__model),
    "os", MAX(visit__device__operating_system),
    "id", MAX(visit__device__enc_uuid)
  ) AS device,
  named_struct
  (
    "country_code", MAX(visit__location__country_code),
    "country_name", MAX(visit__location__country_name),
    "region", MAX(visit__location__region),
    "region_name", MAX(visit__location__region_name),
    "city", MAX(visit__location__enc_city),
    "state", MAX(visit__location__state),
    "zip_code", MAX(visit__location__enc_zip_code),
    "latitude", MAX(visit__location__enc_latitude),
    "longitude", MAX(visit__location__enc_longitude),
    "metro_code", MAX(visit__location__metro_code),
    "timezone", MAX(visit__location__timezone),
    "status", MAX(visit__location__status)
  ) AS location,
  IF(SUBSTR(visit__visit_id, (LENGTH(MAX(visit__device__enc_uuid)) + 1)) > 1, false, true) AS first_time_user,
  MAX(visitor_net.total_visits_last_day) AS total_visits_last_day,
  MAX(visitor_net.total_visits_last_week) AS total_visits_last_week,
  MAX(visitor_net.total_visits_last_month) AS total_visits_last_month,
  MAX(visitor_net.total_visits_last_3_months) AS total_visits_last_3_months,
  MAX(visitor_net.total_visits_last_6_months) AS total_visits_last_6_months,
  MAX(visitor_net.total_visits_last_year) AS total_visits_last_year,
  ((unix_timestamp() - MAX(message__timestamp)) / ${hiveconf:ONE_HOUR_TIMESTAMP}) AS last_visit_hours,
  IF( (MIN(message__timestamp) - (FLOOR(MIN(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) > ${hiveconf:THREE_AM_TIMESTAMP_DIFF} AND (MIN(message__timestamp) - (FLOOR(MIN(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) <= ${hiveconf:TWELVE_PM_TIMESTAMP_DIFF}, "morning", IF( (MIN(message__timestamp) - (FLOOR(MIN(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) > ${hiveconf:TWELVE_PM_TIMESTAMP_DIFF} AND (MIN(message__timestamp) - (FLOOR(MIN(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) <= ${hiveconf:SIX_PM_TIMESTAMP_DIFF}, "afternoon", "night")) AS start_time_of_day,
  IF( (MAX(message__timestamp) - (FLOOR(MAX(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) > ${hiveconf:THREE_AM_TIMESTAMP_DIFF} AND (MAX(message__timestamp) - (FLOOR(MAX(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) <= ${hiveconf:TWELVE_PM_TIMESTAMP_DIFF}, "morning", IF( (MAX(message__timestamp) - (FLOOR(MAX(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) > ${hiveconf:TWELVE_PM_TIMESTAMP_DIFF} AND (MAX(message__timestamp) - (FLOOR(MAX(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) <= ${hiveconf:SIX_PM_TIMESTAMP_DIFF}, "afternoon", "night")) AS end_time_of_day,
  from_unixtime(MIN(message__timestamp)) AS start_timestamp,
  from_unixtime(MAX(message__timestamp)) AS end_timestamp,
  (MAX(message__timestamp) - MIN(message__timestamp)) AS visit_duration,
  NULL AS total_vod_video_starts,
  NULL AS total_live_video_starts,
  NULL AS total_download_video_starts,
  NULL AS total_vod_view_duration_s,
  NULL AS total_live_view_duration_s,
  NULL AS total_download_view_duration_s,
  NULL AS total_live_buffering_duration_ms,
  NULL AS total_live_buffering_events,
  NULL AS vod_buffering_duration_ms,
  NULL AS vod_buffering_events,
  NULL AS download_buffering_duration_ms,
  NULL AS download_buffering_events,
  NULL AS total_live_stream_failures,
  NULL AS total_vod_stream_failures,
  NULL AS total_download_failures,
  NULL AS total_live_stream_restarts,
  NULL AS total_vod_stream_restarts,
  NULL AS total_download_restarts,
  NULL AS total_live_bitrate_upshifts,
  NULL AS total_live_bitrate_downshifts,
  NULL AS total_vod_bitrate_upshifts,
  NULL AS total_vod_bitrate_downshifts,
  NULL AS max_live_bitrate,
  NULL AS max_vod_bitrate,
  NULL AS max_download_bitrate,
  NULL AS min_live_bitrate,
  NULL AS min_vod_bitrate,
  NULL AS min_download_bitrate,
  NULL AS avg_live_bitrate,
  NULL AS avg_vod_bitrate,
  NULL AS avg_download_bitrate,
  NULL AS total_live_startup_events,
  NULL AS total_live_startup_duration_ms,
  NULL AS total_vod_startup_events,
  NULL AS total_vod_startup_duration_ms,
  NULL AS total_download_startup_events,
  NULL AS total_download_startup_duration_ms,
  NULL AS total_download_add_to_queues,
  NULL AS total_title_details_views,
  MAP("search", CAST(SUM(IF(LOWER(message__name) LIKE "search%", 1, 0)) AS INT)) AS total_conversions_by_type,
  ${env:ENVIRONMENT}.CountArrayToMap((COLLECT_LIST(state__view__current_page__section))) AS total_page_views_by_section,
  MAP(NULL, 0) AS total_feature_usage_counts_by_type,
  MAP(NULL, 0) AS login_errors_service,
  MAP(NULL, 0) AS total_service_exceptions,
  ARRAY(NULL) AS visit_path,
  NULL AS total_download_completes,
  NULL AS total_download_starts,
  SUM(IF((visit__login__failed_attempts IS NULL), 0, visit__login__failed_attempts)) AS total_login_failures,
  COLLECT_LIST(CAST(visit__login__login_duration AS DOUBLE)) AS login_durations,
  YEAR(FROM_UNIXTIME(MAX(message__timestamp))) AS year
FROM
  ${env:TMP_db}.net_events_daily
LEFT OUTER JOIN ${env:TMP_db}.visitor_net ON (visitor_net.visitor_id = net_events_daily.visit__device__enc_uuid)
GROUP BY
  visit__visit_id;

DROP TABLE IF EXISTS ${env:TMP_db}.visitor_net;
DROP TABLE IF EXISTS ${env:TMP_db}.net_events_daily;
DROP TABLE IF EXISTS ${env:TMP_db}.net_events_existing;
DROP TABLE IF EXISTS ${env:TMP_db}.net_visits_agg;
