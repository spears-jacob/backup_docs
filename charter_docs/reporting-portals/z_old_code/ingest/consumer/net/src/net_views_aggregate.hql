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
SET MILLI_SECOND=1000;
SET NUMBER_OF_DAYS=1;

DROP TABLE IF EXISTS ${env:TMP_db}.net_view_exclude_views;
CREATE TABLE ${env:TMP_db}.net_view_exclude_views(view_id STRING);
INSERT INTO TABLE ${env:TMP_db}.net_view_exclude_views
  SELECT DISTINCT
    net_views_agg.view_id
  FROM
    net_views_agg
  WHERE
    net_views_agg.year >= '${hiveconf:LAST_YEAR}';

SET hive.execution.engine=mr;

DROP TABLE IF EXISTS ${env:TMP_db}.net_views_daily;
CREATE TABLE ${env:TMP_db}.net_views_daily LIKE net_events;
INSERT INTO TABLE ${env:TMP_db}.net_views_daily PARTITION(partition_date)
  SELECT
    net_events.*
  FROM
    net_events
  WHERE
    '${hiveconf:LAST_DATE}' IS NOT NULL
    AND CAST(net_events.partition_date AS DATE) BETWEEN CAST('${hiveconf:LAST_DATE}' AS DATE) AND DATE_ADD(CAST('${hiveconf:LAST_DATE}' AS DATE), ${hiveconf:NUMBER_OF_DAYS})
    AND NOT EXISTS (
      SELECT net_view_exclude_views.view_id
      FROM ${env:TMP_db}.net_view_exclude_views
      WHERE net_view_exclude_views.view_id = net_events.state__content__stream__view_id
    );

DROP TABLE IF EXISTS ${env:TMP_db}.viewers_net;
CREATE TABLE ${env:TMP_db}.viewers_net
(
  viewers_id STRING,
  total_views_last_day INT,
  total_views_last_week INT,
  total_views_last_month INT,
  total_views_last_3_months INT,
  total_views_last_6_months INT,
  total_views_last_year INT
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as orc
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

INSERT OVERWRITE TABLE ${env:TMP_db}.viewers_net
SELECT
   viewers_id,
   SUM(IF(total_views_last_day > 0, 1, 0)) AS total_views_last_day,
   SUM(IF(total_views_last_week > 0, 1, 0)) AS total_views_last_week,
   SUM(IF(total_views_last_month > 0, 1, 0)) AS total_views_last_month,
   SUM(IF(total_views_last_3_months > 0, 1, 0)) AS total_views_last_3_months,
   SUM(IF(total_views_last_6_months > 0, 1, 0)) AS total_views_last_6_months,
   SUM(IF(total_views_last_year > 0, 1, 0)) AS total_views_last_year
FROM (
  SELECT
    device.id AS viewers_id,
    view_id AS view_id,
    SUM(IF(start_date_time >= unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}'), 1, 0)) AS total_views_last_day,
    SUM(IF(start_date_time >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_WEEK_TIMESTAMP}), 1, 0)) AS total_views_last_week,
    SUM(IF(start_date_time >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_MONTH_TIMESTAMP}), 1, 0)) AS total_views_last_month,
    SUM(IF(start_date_time >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_QUARTER_TIMESTAMP}), 1, 0)) AS total_views_last_3_months,
    SUM(IF(start_date_time >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_HALFYEAR_TIMESTAMP}), 1, 0)) AS total_views_last_6_months,
    SUM(IF(start_date_time >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_YEAR_TIMESTAMP}), 1, 0)) AS total_views_last_year
  FROM
    net_views_agg
  WHERE
    device.id IS NOT NULL AND
    device.id <> "" AND
    start_date_time IS NOT NULL AND
    start_date_time >= (unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}') - ${hiveconf:ONE_YEAR_TIMESTAMP}) AND 
    start_date_time <= unix_timestamp('${hiveconf:LAST_DATE_TIMESTAMP}')
  GROUP BY device.id, view_id
) AS viewers_net_temp
GROUP BY viewers_id;

INSERT INTO TABLE net_views_agg PARTITION(year)
SELECT
  MAX(visit__visit_id) AS visit_id,
  state__content__stream__view_id AS view_id,
    MAX(state__content__stream__type) AS view_type,
  named_struct
  (
    "user_id", MAX(visit__user__enc_id),
    "account_id", MAX(visit__account__enc_account_number),
    "test_account", MAX(visit__user__is_test_user),
    "headend", "",
    "onNet", IF(MAX(visit__connection__network_status) == "OnNet", true, false),
    "subscriptionType", MAX(visit__account__subscription__service_level)
  ) AS customer,
  named_struct
  (
    "type", MAX(visit__device__model),
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

  named_struct
  (
     "type", MAX(state__content__details__type),
     "title", MAX(state__content__details__title),
     "episode_number", MAX(state__content__details__episode_number),
     "season_number", MAX(state__content__details__season_number),
     "available_date", MAX(state__content__details__available_date),
     "closed_captioned", MAX(state__content__details__closed_captioned),
     "long_form", MAX(state__content__details__long_form),
     "runtime", MAX(state__content__details__runtime),
     "expiration_date", MAX(state__content__details__expiration_date),
     "original_air_date", MAX(state__content__details__original_air_date),
     "original_network_name", MAX(state__content__details__original_network_name),
     "rating", MAX(state__content__details__rating),
     "description", MAX(state__content__details__description),
     "year", MAX(state__content__details__year),
     "episode_title",MAX(state__content__details__episode_title),
     "hd", MAX(state__content__details__hd),
     "actors", MAX(state__content__details__actors),
     "genres", MAX(state__content__details__genres)
  ) AS program,

  named_struct
  (
     "id", MAX(state__content__identifiers__series_id),
     "id_type", MAX(state__content__identifiers__series_id_type)
  ) AS series,

   named_struct
   (
       "call_sign", MAX(state__content__programmer__callsign),
       "id", MAX(state__content__programmer__id),
       "vod", named_struct
       (
          "provider_id", MAX(state__content__programmer__vod__provider_id),
        "provider_name", MAX(state__content__programmer__vod__provider_name),
        "product", MAX(state__content__programmer__vod__product)
       ),
      "linear", named_struct
    (
        "channel_name", MAX(state__content__programmer__linear__channel_name),
      "network_name", MAX(state__content__programmer__linear__network_name),
      "channel_category", MAX(state__content__programmer__linear__channel_category),
      "channel_number", MAX(state__content__programmer__linear__channel_number)
     )
   ) AS programmer,
  MAX(visit__connection__type) AS network_type,
    null AS conversion_type,
  MAX(viewers_net.total_views_last_day) AS total_views_last_day,
  MAX(viewers_net.total_views_last_week) AS total_views_last_week,
  MAX(viewers_net.total_views_last_month) AS total_views_last_month,
  MAX(viewers_net.total_views_last_3_months) AS total_views_last_3_months,
  MAX(viewers_net.total_views_last_6_months) AS total_views_last_6_months,
  MAX(viewers_net.total_views_last_year) AS total_views_last_year,
  ((MAX(message__timestamp) - MIN(message__timestamp)) / ${hiveconf:ONE_HOUR_TIMESTAMP}) AS last_view_hours,
  IF( (MIN(message__timestamp) - (FLOOR(MIN(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) > ${hiveconf:THREE_AM_TIMESTAMP_DIFF} AND (MIN(message__timestamp) - (FLOOR(MIN(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) <= ${hiveconf:TWELVE_PM_TIMESTAMP_DIFF}, "morning", IF( (MIN(message__timestamp) - (FLOOR(MIN(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) > ${hiveconf:TWELVE_PM_TIMESTAMP_DIFF} AND (MIN(message__timestamp) - (FLOOR(MIN(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) <= ${hiveconf:SIX_PM_TIMESTAMP_DIFF}, "afternoon", "night")) AS start_time_of_day,
  IF( (MAX(message__timestamp) - (FLOOR(MAX(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) > ${hiveconf:THREE_AM_TIMESTAMP_DIFF} AND (MAX(message__timestamp) - (FLOOR(MAX(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) <= ${hiveconf:TWELVE_PM_TIMESTAMP_DIFF}, "morning", IF( (MAX(message__timestamp) - (FLOOR(MAX(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) > ${hiveconf:TWELVE_PM_TIMESTAMP_DIFF} AND (MAX(message__timestamp) - (FLOOR(MAX(message__timestamp) / ${hiveconf:ONE_DAY_TIMESTAMP}) * ${hiveconf:ONE_DAY_TIMESTAMP})) <= ${hiveconf:SIX_PM_TIMESTAMP_DIFF}, "afternoon", "night")) AS end_time_of_day,
  FROM_UNIXTIME(MIN(message__timestamp)) AS start_date_time,
  FROM_UNIXTIME(MAX(message__timestamp)) AS end_date_time,
  NULL AS view_duration_sec,
  NULL AS total_buffering_duration_ms,
  NULL AS total_buffering_events,
  Array() AS buffering_durations_ms,
  SUM(IF(message__name == "Play Load Failure",1,0)) AS total_stream_failures,
  NULL AS total_stream_restarts,
  NULL AS total_bitrate_upshifts,
  NULL AS total_bitrate_downshifts,
  IF((MAX(IF((message__name == "Play Load Success" AND message__timestamp IS NOT NULL), message__timestamp, 0)) - MIN(IF((message__name == "Launch Player" AND message__timestamp IS NOT NULL), message__timestamp, 0))) >= 0, (MAX(IF((message__name == "Play Load Success" AND message__timestamp IS NOT NULL), message__timestamp, 0)) - MIN(IF((message__name == "Launch Player" AND message__timestamp IS NOT NULL), message__timestamp, 0))) * ${hiveconf:MILLI_SECOND}, 0 ) AS startup_time_ms,
  MAP(0.0,0) AS view_bitrates,
  NULL AS max_bitrate,
  NULL AS min_bitrate,
  NULL AS avg_bitrate,
  YEAR(FROM_UNIXTIME(MAX(message__timestamp))) AS year

FROM
  ${env:TMP_db}.net_views_daily
LEFT OUTER JOIN
  ${env:TMP_db}.viewers_net ON (viewers_net.viewers_id = net_views_daily.visit__device__enc_uuid)
GROUP BY
  state__content__stream__view_id;

SET hive.execution.engine=tez;

TRUNCATE TABLE ${env:TMP_db}.net_views_agg_last_date;
INSERT INTO TABLE ${env:TMP_db}.net_views_agg_last_date SELECT DATE_ADD('${hiveconf:LAST_DATE}', ${hiveconf:NUMBER_OF_DAYS}) FROM ${env:TMP_db}.net_visits_agg_last_date LIMIT 1;

DROP TABLE ${env:TMP_db}.viewers_net;
DROP TABLE IF EXISTS ${env:TMP_db}.net_views_daily;
DROP TABLE IF EXISTS ${env:TMP_db}.net_view_exclude_views;
