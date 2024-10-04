-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- All stream level playback metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
set RUN_DATE = 2017-04-10;

with base_data as (
  select
    state__content__stream__playback_id AS stream_id,
    visit__visit_id AS visit_id,
    visit__device__uuid AS device_id,
    visit__account__account_number AS account_number,
    visit__account__account_billing_id as billing_id,
    CASE
      WHEN visit__account__details__mso IS NULL THEN 'MSO-MISSING'
      WHEN visit__account__details__mso = 'NONECAPTURED' THEN 'L-TWC'
      WHEN visit__account__details__mso = '' THEN 'MSO-MISSING'
      WHEN visit__account__details__mso = 'CHARTER' THEN 'L-CHTR'
      WHEN visit__account__details__mso = 'BH' THEN 'L-BHN'
      WHEN visit__account__details__mso = 'TWC' THEN 'L-TWC'
      ELSE visit__account__details__mso	END as mso,
    visit__application_details__application_type as application_type,
    lower(visit__device__device_type) as device_type,
    visit__connection__connection_type as connection_type,
    visit__connection__network_status as network_status,
    state__content__stream__playback_type as playback_type,
    -- playback metrics below
    IF(message__name = 'loginStart' AND operation__operation_type = 'tokenExchange' AND visit__account__details__mso = 'CHARTER', 1, 0) AS token_exchange_attempts,
    IF(message__name = 'playbackStart'
      AND state__previous_state__name <> 'failed'
      and state__content__stream__playback_type IN ('vod', 'linear'), 1, 0) as stream_init_starts,
    IF(message__name = 'playbackFailure'
      AND state__previous_state__name = 'initiating'
      and state__content__stream__playback_type IN ('vod', 'linear'), 1, 0) as stream_init_failures,
    IF(message__name = 'playbackFailure'
      AND state__previous_state__name <>'initiating'
      and state__content__stream__playback_type IN ('vod', 'linear'), 1, 0) as stream_noninit_failures,
    IF(message__name = 'playbackStart'
      AND state__content__stream__playback_type IN ('vod', 'linear')
      AND state__playback__tune_time_ms BETWEEN 0 AND 900000, state__playback__tune_time_ms, NULL) as tune_time_ms,
    IF(message__name NOT IN ('playbackSelect', 'playbackStreamUriAcquired')
      AND state__content__stream__playback_type IN ('vod', 'linear'), IF(COALESCE(state__playback__heartbeat__content_elapsed_ms, 0) <= 60000, COALESCE(state__playback__heartbeat__content_elapsed_ms, 0), 60000), 0) / 1000
     as watch_time_sec,
    IF(message__name = 'bufferingStop'
      and state__content__stream__playback_type IN ('vod', 'linear')
      AND state__playback__buffering_time_ms BETWEEN 0 AND 600000, state__playback__buffering_time_ms, 0) / 1000 AS buffering_time_sec,
    IF(state__content__stream__playback_type IN ('vod', 'linear'), (COALESCE(state__playback__bitrate__current_bitrate_mbps, 0) * COALESCE(state__playback__bitrate__content_elapsed_at_current_bitrate_ms, 0) / 1000) + (COALESCE(state__playback__bitrate__previous_bitrate_mbps, 0) * COALESCE(state__playback__bitrate__content_elapsed_at_previous_bitrate_ms, 0) / 1000), 0) AS bandwidth_consumed_mb,
    IF(state__content__stream__playback_type IN ('vod', 'linear'), COALESCE(state__playback__bitrate__content_elapsed_at_current_bitrate_ms, 0) / 1000 + COALESCE(state__playback__bitrate__content_elapsed_at_previous_bitrate_ms, 0) / 1000, 0) as bitrate_content_elapsed_sec,
    IF(IF(state__content__stream__playback_type IN ('vod', 'linear'), COALESCE(state__playback__bitrate__content_elapsed_at_current_bitrate_ms, 0) / 1000 + COALESCE(state__playback__bitrate__content_elapsed_at_previous_bitrate_ms, 0) / 1000, 0) != 0,
    (IF(state__content__stream__playback_type IN ('vod', 'linear'), (COALESCE(state__playback__bitrate__current_bitrate_mbps, 0) * COALESCE(state__playback__bitrate__content_elapsed_at_current_bitrate_ms, 0) / 1000) + (COALESCE(state__playback__bitrate__previous_bitrate_mbps, 0) * COALESCE(state__playback__bitrate__content_elapsed_at_previous_bitrate_ms, 0) / 1000), 0)) / IF(state__content__stream__playback_type IN ('vod', 'linear'), COALESCE(state__playback__bitrate__content_elapsed_at_current_bitrate_ms, 0) / 1000 + COALESCE(state__playback__bitrate__content_elapsed_at_previous_bitrate_ms, 0) / 1000, 0), 0) AS avg_bitrate_mbps,
    IF(message__name  = 'bufferingStart'
      and state__content__stream__playback_type IN ('vod', 'linear'), 1, 0) as buffering_starts,
    IF(message__name  = 'playbackBitRateUpshift'
      and state__content__stream__playback_type IN ('vod', 'linear'), 1, 0) as bitrate_upshifts,
    IF(message__name  = 'playbackBitRateDownshift'
      and state__content__stream__playback_type IN ('vod', 'linear'), 1, 0) as bitrate_downshifts,
    DATE(from_utc_timestamp(concat(partition_date, ' ', lpad(hour, 2, '0'), ':00:00'), 'America/Denver')) as denver_date
  from prod.venona_events
  where
    ( (partition_date = '${hiveconf:RUN_DATE}' and hour >= '06')
    or (partition_date = date_add('${hiveconf:RUN_DATE}', 1) and hour < '06') )
    and state__content__stream__playback_id <> ''
    and isnotnull(state__content__stream__playback_id)
    and visit__account__details__mso <> ''
    and isnotnull(visit__account__details__mso)
)
select
  CASE WHEN (grouping_id & 2) != 0 THEN mso ELSE 'All Companies' END AS mso,
  CASE WHEN (grouping_id & 4) != 0 THEN application_type ELSE 'All Applications' END AS application_type,
  CASE WHEN (grouping_id & 8) != 0 THEN device_type ELSE 'All Devices' END AS device_type,
  CASE WHEN (grouping_id & 16) != 0 THEN connection_type ELSE 'All Connections' END AS connection_type,
  CASE WHEN (grouping_id & 32) != 0 THEN network_status ELSE 'All Network Statuses' END AS network_status,
  CASE WHEN (grouping_id & 64) != 0 THEN playback_type ELSE 'All Playback Types' END AS playback_type,
  grouping_id,
  metric_name,
  metric_value,
  denver_date
FROM (
  SELECT
  '${hiveconf:RUN_DATE}' as denver_date,
  mso,
  application_type,
  device_type,
  connection_type,
  network_status,
  playback_type,
  CAST(grouping__id AS int) AS grouping_id,
  MAP(
    'stream_init_starts', sum(stream_init_starts),
    'stream_init_failures', sum(stream_init_failures),
    'stream_init_total', sum(stream_init_starts) + sum(stream_init_failures),
    'stream_init_success_rate', sum(stream_init_starts) / (sum(stream_init_starts) + sum(stream_init_failures)),
    'avg_bitrate_mbps', if(sum(bitrate_content_elapsed_sec) != 0, sum(bandwidth_consumed_mb) / sum(bitrate_content_elapsed_sec), 0),
    'bitrate_downshifts', sum(bitrate_downshifts),
    'content_consumed_hours_per_stream', (SUM(watch_time_sec) / 3600) / SUM(stream_init_starts),
    'content_consumed_hours', SUM(watch_time_sec) / 3600,
    'median_tune_time_seconds', PERCENTILE_APPROX(tune_time_ms, 0.5) / 1000,
    'avg_tune_time_seconds', avg(tune_time_ms / 1000),
    'total_buffering_time_sec', SUM(buffering_time_sec),
    'buffering_ratio', SUM(watch_time_sec) / SUM(buffering_time_sec)
  ) AS tmp_map
from base_data
GROUP BY
  '${hiveconf:RUN_DATE}',
  mso,
  application_type,
  device_type,
  connection_type,
  network_status,
  playback_type
  GROUPING SETS (
    ('${hiveconf:RUN_DATE}'),
    ('${hiveconf:RUN_DATE}', connection_type),
    ('${hiveconf:RUN_DATE}', connection_type, network_status),
    ('${hiveconf:RUN_DATE}', connection_type, network_status, playback_type),
    ('${hiveconf:RUN_DATE}', connection_type, playback_type),
    ('${hiveconf:RUN_DATE}', network_status),
    ('${hiveconf:RUN_DATE}', network_status, playback_type),
    ('${hiveconf:RUN_DATE}', playback_type),
    ('${hiveconf:RUN_DATE}', mso),
    ('${hiveconf:RUN_DATE}', mso, connection_type),
    ('${hiveconf:RUN_DATE}', mso, connection_type, network_status),
    ('${hiveconf:RUN_DATE}', mso, connection_type, playback_type),
    ('${hiveconf:RUN_DATE}', mso, connection_type, network_status, playback_type),
    ('${hiveconf:RUN_DATE}', mso, network_status),
    ('${hiveconf:RUN_DATE}', mso, network_status, playback_type),
    ('${hiveconf:RUN_DATE}', mso, playback_type),
    ('${hiveconf:RUN_DATE}', application_type),
    ('${hiveconf:RUN_DATE}', application_type, connection_type),
    ('${hiveconf:RUN_DATE}', application_type, connection_type, network_status),
    ('${hiveconf:RUN_DATE}', application_type, connection_type, playback_type),
    ('${hiveconf:RUN_DATE}', application_type, connection_type, network_status, playback_type),
    ('${hiveconf:RUN_DATE}', application_type, network_status),
    ('${hiveconf:RUN_DATE}', application_type, network_status, playback_type),
    ('${hiveconf:RUN_DATE}', application_type, playback_type),
    ('${hiveconf:RUN_DATE}', application_type, device_type),
    ('${hiveconf:RUN_DATE}', mso, application_type),
    ('${hiveconf:RUN_DATE}', mso, application_type, connection_type),
    ('${hiveconf:RUN_DATE}', mso, application_type, connection_type, network_status),
    ('${hiveconf:RUN_DATE}', mso, application_type, connection_type, playback_type),
    ('${hiveconf:RUN_DATE}', mso, application_type, connection_type, network_status, playback_type),
    ('${hiveconf:RUN_DATE}', mso, application_type, network_status),
    ('${hiveconf:RUN_DATE}', mso, application_type, network_status, playback_type),
    ('${hiveconf:RUN_DATE}', mso, application_type, playback_type),
    ('${hiveconf:RUN_DATE}', mso, application_type, device_type),
    ('${hiveconf:RUN_DATE}', mso, application_type, device_type, connection_type),
    ('${hiveconf:RUN_DATE}', mso, application_type, device_type, connection_type, network_status),
    ('${hiveconf:RUN_DATE}', mso, application_type, device_type, connection_type, playback_type),
    ('${hiveconf:RUN_DATE}', mso, application_type, device_type, connection_type, network_status, playback_type),
    ('${hiveconf:RUN_DATE}', mso, application_type, device_type, network_status),
    ('${hiveconf:RUN_DATE}', mso, application_type, device_type, network_status, playback_type),
    ('${hiveconf:RUN_DATE}', mso, application_type, device_type, playback_type))
) a
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- Per stream level playback metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

union

select
  CASE WHEN (grouping_id & 2) != 0 THEN mso ELSE 'All Companies' END AS mso,
  CASE WHEN (grouping_id & 4) != 0 THEN application_type ELSE 'All Applications' END AS application_type,
  CASE WHEN (grouping_id & 8) != 0 THEN device_type ELSE 'All Devices' END AS device_type,
  CASE WHEN (grouping_id & 16) != 0 THEN connection_type ELSE 'All Connections' END AS connection_type,
  CASE WHEN (grouping_id & 32) != 0 THEN network_status ELSE 'All Network Statuses' END AS network_status,
  CASE WHEN (grouping_id & 64) != 0 THEN playback_type ELSE 'All Playback Types' END AS playback_type,
  grouping_id,
  metric_name,
  metric_value
FROM (
SELECT
  '${hiveconf:RUN_DATE}' as denver_date,
  mso,
  application_type,
  device_type,
  connection_type,
  network_status,
  playback_type,
  CAST(grouping__id AS int) AS grouping_id,
  MAP (
    'broken_stream_perc', SUM(CASE WHEN stream_init_starts > 0 AND stream_noninit_failures > 0 THEN 1 ELSE 0 END) / SUM(CASE WHEN stream_init_starts > 0 THEN 1 ELSE 0 END),
    'broken_streams', SUM(CASE WHEN stream_init_starts > 0 AND stream_noninit_failures > 0 THEN 1 ELSE 0 END),
    'bitrate_bucket_0.6', sum(case when avg_bitrate_mbps > 0 and avg_bitrate_mbps < 0.6 then 1 else 0 end),
    'bitrate_bucket_0.6_0.9', sum(case when avg_bitrate_mbps >= 0.6 and avg_bitrate_mbps < 0.9 then 1 else 0 end),
    'bitrate_bucket_0.9_1.4', sum(case when avg_bitrate_mbps >= 0.9 and avg_bitrate_mbps < 1.4 then 1 else 0 end),
    'bitrate_bucket_1.4_2.4', sum(case when avg_bitrate_mbps >= 1.4 and avg_bitrate_mbps < 2.4 then 1 else 0 end),
    'bitrate_bucket_2.4', sum(case when avg_bitrate_mbps >= 2.4 then 1 else 0 end)
  ) AS tmp_map
  from (
    select
      max(mso) as mso,
      max(application_type) as application_type,
      max(device_type) as device_type,
      max(connection_type) as connection_type,
      max(network_status) as network_status,
      max(playback_type) playback_type,
      if(sum(bitrate_content_elapsed_sec) != 0, sum(bandwidth_consumed_mb) / sum(bitrate_content_elapsed_sec), 0) as avg_bitrate_mbps,
      sum(stream_init_starts) stream_init_starts,
      sum(stream_noninit_failures) stream_noninit_failures
    from base_data
    group by stream_id
) stream_agg
GROUP BY
  '${hiveconf:RUN_DATE}',
  mso,
  application_type,
  device_type,
  connection_type,
  network_status,
  playback_type
  GROUPING SETS (
  ('${hiveconf:RUN_DATE}'),
  ('${hiveconf:RUN_DATE}', connection_type),
  ('${hiveconf:RUN_DATE}', connection_type, network_status),
  ('${hiveconf:RUN_DATE}', connection_type, network_status, playback_type),
  ('${hiveconf:RUN_DATE}', connection_type, playback_type),
  ('${hiveconf:RUN_DATE}', network_status),
  ('${hiveconf:RUN_DATE}', network_status, playback_type),
  ('${hiveconf:RUN_DATE}', playback_type),
  ('${hiveconf:RUN_DATE}', mso),
  ('${hiveconf:RUN_DATE}', mso, connection_type),
  ('${hiveconf:RUN_DATE}', mso, connection_type, network_status),
  ('${hiveconf:RUN_DATE}', mso, connection_type, playback_type),
  ('${hiveconf:RUN_DATE}', mso, connection_type, network_status, playback_type),
  ('${hiveconf:RUN_DATE}', mso, network_status),
  ('${hiveconf:RUN_DATE}', mso, network_status, playback_type),
  ('${hiveconf:RUN_DATE}', mso, playback_type),
  ('${hiveconf:RUN_DATE}', application_type),
  ('${hiveconf:RUN_DATE}', application_type, connection_type),
  ('${hiveconf:RUN_DATE}', application_type, connection_type, network_status),
  ('${hiveconf:RUN_DATE}', application_type, connection_type, playback_type),
  ('${hiveconf:RUN_DATE}', application_type, connection_type, network_status, playback_type),
  ('${hiveconf:RUN_DATE}', application_type, network_status),
  ('${hiveconf:RUN_DATE}', application_type, network_status, playback_type),
  ('${hiveconf:RUN_DATE}', application_type, playback_type),
  ('${hiveconf:RUN_DATE}', application_type, device_type),
  ('${hiveconf:RUN_DATE}', mso, application_type),
  ('${hiveconf:RUN_DATE}', mso, application_type, connection_type),
  ('${hiveconf:RUN_DATE}', mso, application_type, connection_type, network_status),
  ('${hiveconf:RUN_DATE}', mso, application_type, connection_type, playback_type),
  ('${hiveconf:RUN_DATE}', mso, application_type, connection_type, network_status, playback_type),
  ('${hiveconf:RUN_DATE}', mso, application_type, network_status),
  ('${hiveconf:RUN_DATE}', mso, application_type, network_status, playback_type),
  ('${hiveconf:RUN_DATE}', mso, application_type, playback_type),
  ('${hiveconf:RUN_DATE}', mso, application_type, device_type),
  ('${hiveconf:RUN_DATE}', mso, application_type, device_type, connection_type),
  ('${hiveconf:RUN_DATE}', mso, application_type, device_type, connection_type, network_status),
  ('${hiveconf:RUN_DATE}', mso, application_type, device_type, connection_type, playback_type),
  ('${hiveconf:RUN_DATE}', mso, application_type, device_type, connection_type, network_status, playback_type),
  ('${hiveconf:RUN_DATE}', mso, application_type, device_type, network_status),
  ('${hiveconf:RUN_DATE}', mso, application_type, device_type, network_status, playback_type),
  ('${hiveconf:RUN_DATE}', mso, application_type, device_type, playback_type))
) a
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;
