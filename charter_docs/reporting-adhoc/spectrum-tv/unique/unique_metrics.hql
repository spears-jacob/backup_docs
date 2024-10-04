-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
--
-- Unique counts metrics
--
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
set RUN_DATE = 2017-04-10;

with base_data as (
  select
    if(message__name = 'playbackStart', state__content__stream__playback_id, NULL) AS stream_id,
    if((message__name = 'loginStop' AND operation__success = TRUE) OR message__name = 'playbackHeartbeat', visit__visit_id, NULL) AS visit_id,
    if((message__name = 'loginStop' AND operation__success = TRUE) OR message__name = 'playbackHeartbeat', visit__device__uuid, NULL) AS device_id,
    if((message__name = 'loginStop' AND operation__success = TRUE) OR message__name = 'playbackHeartbeat', visit__account__account_number, NULL) AS account_number,
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
    state__content__stream__playback_type as playback_type
  from prod.venona_events
  where
    ( (partition_date = '${hiveconf:RUN_DATE}' and hour >= '07')
    or (partition_date = date_add('${hiveconf:RUN_DATE}', 1) and hour < '07') )
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
    MAP(
      'unique_devices', SIZE(COLLECT_SET(device_id)),
      'unique_households', SIZE(COLLECT_SET(account_number)),
      'unique_visits', SIZE(COLLECT_SET(visit_id)),
      'set_unique_streams', size(COLLECT_SET(stream_id))
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
LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value;
