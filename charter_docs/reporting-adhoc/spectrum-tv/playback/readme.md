# Playback metrics

This includes all playback metrics.

## Filters
All playback metrics has the following filters:
- Linear and vod: `state__content__stream__playback_type in ('vod', 'linear')`
- Streams with stream id: `state__content__stream__playback_id <> '' and isnotnull(state__content__stream__playback_id)`
- Streams with MSOs assigned: `visit__account__details__mso <> '' and isnotnull(visit__account__details__mso)`

## Grouping Levels
All playback metrics has the following grouping levels:
- MSO. `MSO-MISSING` should be all filtered out if filter is applied
```
CASE
  WHEN visit__account__details__mso IS NULL THEN 'MSO-MISSING'
  WHEN visit__account__details__mso = 'NONECAPTURED' THEN 'L-TWC'
  WHEN visit__account__details__mso = '' THEN 'MSO-MISSING'
  WHEN visit__account__details__mso = 'CHARTER' THEN 'L-CHTR'
  WHEN visit__account__details__mso = 'BH' THEN 'L-BHN'
  WHEN visit__account__details__mso = 'TWC' THEN 'L-TWC'
  ELSE visit__account__details__mso
END as mso
```
- Application Type:
  - without adroid version breakdown: `visit__application_details__application_type as application_type`,
  - or with android version break down:
```
CASE
  WHEN visit__application_details__application_type = 'Android' THEN (
case when visit__device__operating_system LIKE '%4.0%' OR visit__device__operating_system LIKE '%4.1%' OR visit__device__operating_system LIKE '%4.2%' OR visit__device__operating_system LIKE '%4.3%' THEN 'Android 4.1 - 4.3' ELSE 'Android 4.4+'
END) ELSE visit__application_details__application_type
END as application_type
```
- Device type: `lower(visit__device__device_type) as device_type`
- Connection Type: `visit__connection__connection_type as connection_type`
- Network Status: `visit__connection__network_status as network_status`
- Playback Type: `state__content__stream__playback_type as playback_type`

## Calculation

### Total Events Level

All stream level metrics are calculated on top of total events.
- stream_init_starts: total started streams, `sum(stream_init_starts)`
  - stream_init_starts: `IF(message__name = 'playbackStart' AND state__previous_state__name <> 'failed' and state__content__stream__playback_type IN ('vod', 'linear'), 1, 0)`
- stream_init_failures: total streams that failed during initiating, `sum(stream_init_failures)`
  - stream_init_failures: `IF(message__name = 'playbackFailure' AND state__previous_state__name = 'initiating' and state__content__stream__playback_type IN ('vod', 'linear'), 1, 0)`
- stream_init_total: total streams, `sum(stream_init_starts) + sum(stream_init_failures)`
- stream_init_success_rate: success rate of stream initiaion, `sum(stream_init_starts) / (sum(stream_init_starts) + sum(stream_init_failures))`
- avg_bitrate_mbps: average bitrate of all streams, `if(sum(bitrate_content_elapsed_sec) != 0, sum(bandwidth_consumed_mb) / sum(bitrate_content_elapsed_sec), 0)`
  - bitrate_content_elapsed_sec: `IF(state__content__stream__playback_type IN ('vod', 'linear'), COALESCE(state__playback__bitrate__content_elapsed_at_current_bitrate_ms, 0) / 1000 + COALESCE(state__playback__bitrate__content_elapsed_at_previous_bitrate_ms, 0) / 1000, 0)`
  - bandwidth_consumed_mb: `IF(state__content__stream__playback_type IN ('vod', 'linear'), (COALESCE(state__playback__bitrate__current_bitrate_mbps, 0) * COALESCE(state__playback__bitrate__content_elapsed_at_current_bitrate_ms, 0) / 1000) + (COALESCE(state__playback__bitrate__previous_bitrate_mbps, 0) * COALESCE(state__playback__bitrate__content_elapsed_at_previous_bitrate_ms, 0) / 1000), 0)`
  - With this calculation, bitrate that has 0 second elapsed will be converted to 0 instead of NULL
- bitrate_downshifts: total times when there is a bitrate downshift event, `IF(message__name  = 'playbackBitRateDownshift' and state__content__stream__playback_type IN ('vod', 'linear'), 1, 0)`
- content_consumed_hours_per_stream: total hours of content elapsed divided by total numbers of unique streams, `(SUM(watch_time_sec) / 3600) / SUM(stream_init_starts)`
  - watch_time_sec: `IF(message__name NOT IN ('playbackSelect', 'playbackStreamUriAcquired') AND state__content__stream__playback_type IN ('vod', 'linear'), IF(COALESCE(state__playback__heartbeat__content_elapsed_ms, 0) <= 60000, COALESCE(state__playback__heartbeat__content_elapsed_ms, 0), 60000), 0) / 1000`
- content_consumed_hours: total hours of content consumed across all streams, `SUM(watch_time_sec) / 3600`
- median_tune_time_seconds: the 50 percentile of tune time for all streams in seconds, `PERCENTILE_APPROX(tune_time_ms, 0.5) / 1000`
  - tune_time_ms: `IF(message__name = 'playbackStart' AND state__content__stream__playback_type IN ('vod', 'linear') AND state__playback__tune_time_ms BETWEEN 0 AND 900000, state__playback__tune_time_ms, NULL)`
- avg_tune_time_seconds: the average tune time across all streams, `avg(tune_time_ms / 1000)`
- total_buffering_time_sec: total seconds spent on buffering events, `SUM(buffering_time_sec)`
  - buffering_time_sec: `IF(message__name = 'bufferingStop' and state__content__stream__playback_type IN ('vod', 'linear') AND state__playback__buffering_time_ms BETWEEN 0 AND 600000, state__playback__buffering_time_ms, 0) / 1000`
- buffering_ratio: seconds of content elapsed for every second spent on buffering, `SUM(watch_time_sec) / SUM(buffering_time_sec)`

### Per Stream Level

Per stream level metrics are calculated at a per stream level. Different from total streams query, per stream level metrics group by `stream_id` first before calculation.
- broken_stream_perc: out of all started streams, percent of streams that failed after successfully initiated, `SUM(CASE WHEN stream_init_starts > 0 AND stream_noninit_failures > 0 THEN 1 ELSE 0 END) / SUM(CASE WHEN stream_init_starts > 0 THEN 1 ELSE 0 END)`
- broken_streams: streams failed after successfully started, `SUM(CASE WHEN stream_init_starts > 0 AND stream_noninit_failures > 0 THEN 1 ELSE 0 END)`
- bitrate_bucket_0.6: total number of streams with bitrate between 0 and 0.6, `sum(case when avg_bitrate_mbps > 0 and avg_bitrate_mbps < 0.6 then 1 else 0 end)`,
- bitrate_bucket_0.6_0.9, total number of streams with bitrate between 0.6 and 0.9, `sum(case when avg_bitrate_mbps >= 0.6 and avg_bitrate_mbps < 0.9 then 1 else 0 end)`,
- bitrate_bucket_0.9_1.4, total number of streams with bitrate between 0.9 and 1.4, `sum(case when avg_bitrate_mbps >= 0.9 and avg_bitrate_mbps < 1.4 then 1 else 0 end)`,
- bitrate_bucket_1.4_2.4, total number of streams with bitrate between 1.4 and 2.4, `sum(case when avg_bitrate_mbps >= 1.4 and avg_bitrate_mbps < 2.4 then 1 else 0 end)`,
- bitrate_bucket_2.4, total number of streams with bitrate bigger than 2.4, `sum(case when avg_bitrate_mbps >= 2.4 then 1 else 0 end)`
  - if avg_bitrate_mbps is calculated without `sum(bitrate_content_elapsed_sec) != 0` if statement, then there will be `NULL` need to be filtered out before apply bitrate bucket

We added bitrate watch time bucket as below
- 'bitrate_0.6_watch_1', `sum(case when avg_bitrate_mbps > 0 and avg_bitrate_mbps < 0.6 and watch_time_sec > 0 and watch_time_sec < 1 then 1 else 0 end)`,
- 'bitrate_0.6_watch_1_15', `sum(case when avg_bitrate_mbps > 0 and avg_bitrate_mbps < 0.6 and watch_time_sec >= 1 and watch_time_sec < 15 then 1 else 0 end)`,
- 'bitrate_0.6_watch_15_30', `sum(case when avg_bitrate_mbps > 0 and avg_bitrate_mbps < 0.6 and watch_time_sec >= 15 and watch_time_sec < 30 then 1 else 0 end)`,
- 'bitrate_0.6_watch_30', `sum(case when avg_bitrate_mbps > 0 and avg_bitrate_mbps < 0.6 and watch_time_sec >= 30 then 1 else 0 end)`,
- 'bitrate_0.6_0.9_watch_1', `sum(case when avg_bitrate_mbps >= 0.6 and avg_bitrate_mbps < 0.9 and watch_time_sec > 0 and watch_time_sec < 1 then 1 else 0 end)`,
- 'bitrate_0.6_0.9_watch_1_15', `sum(case when avg_bitrate_mbps >= 0.6 and avg_bitrate_mbps < 0.9 and watch_time_sec >= 1 and watch_time_sec < 15 then 1 else 0 end)`,
- 'bitrate_0.6_0.9_watch_15_30', `sum(case when avg_bitrate_mbps >= 0.6 and avg_bitrate_mbps < 0.9 and watch_time_sec >= 15 and watch_time_sec < 30 then 1 else 0 end)`,
- 'bitrate_0.6_0.9_watch_30', `sum(case when avg_bitrate_mbps >= 0.6 and avg_bitrate_mbps < 0.9 and watch_time_sec >= 30 then 1 else 0 end)`,
- 'bitrate_0.9_1.4_watch_1', `sum(case when avg_bitrate_mbps >= 0.9 and avg_bitrate_mbps < 1.4 and watch_time_sec > 0 and watch_time_sec < 1 then 1 else 0 end)`,
- 'bitrate_0.9_1.4_watch_1_15', `sum(case when avg_bitrate_mbps >= 0.9 and avg_bitrate_mbps < 1.4 and watch_time_sec >= 1 and watch_time_sec < 15 then 1 else 0 end)`,
- 'bitrate_0.9_1.4_watch_15_30', `sum(case when avg_bitrate_mbps >= 0.9 and avg_bitrate_mbps < 1.4 and watch_time_sec >= 15 and watch_time_sec < 30 then 1 else 0 end)`,
- 'bitrate_0.9_1.4_watch_30', `sum(case when avg_bitrate_mbps >= 0.9 and avg_bitrate_mbps < 1.4 and watch_time_sec >= 30 then 1 else 0 end)`,
- 'bitrate_1.4_2.4_watch_1', `sum(case when avg_bitrate_mbps >= 1.4 and avg_bitrate_mbps < 2.4 and watch_time_sec > 0 and watch_time_sec < 1 then 1 else 0 end)`,
- 'bitrate_1.4_2.4_watch_1_15', `sum(case when avg_bitrate_mbps >= 1.4 and avg_bitrate_mbps < 2.4 and watch_time_sec >= 1 and watch_time_sec < 15 then 1 else 0 end)`,
- 'bitrate_1.4_2.4_watch_15_30', `sum(case when avg_bitrate_mbps >= 1.4 and avg_bitrate_mbps < 2.4 and watch_time_sec >= 15 and watch_time_sec < 30 then 1 else 0 end)`,
- 'bitrate_1.4_2.4_watch_30', `sum(case when avg_bitrate_mbps >= 1.4 and avg_bitrate_mbps < 2.4 and watch_time_sec >= 30 then 1 else 0 end)`,
- 'bitrate_2.4_3.4_watch_1', `sum(case when avg_bitrate_mbps >= 2.4 and avg_bitrate_mbps < 3.4 and watch_time_sec > 0 and watch_time_sec < 1 then 1 else 0 end)`,
- 'bitrate_2.4_3.4_watch_1_15', `sum(case when avg_bitrate_mbps >= 2.4 and avg_bitrate_mbps < 3.4 and watch_time_sec >= 1 and watch_time_sec < 15 then 1 else 0 end)`,
- 'bitrate_2.4_3.4_watch_15_30', `sum(case when avg_bitrate_mbps >= 2.4 and avg_bitrate_mbps < 3.4 and watch_time_sec >= 15 and watch_time_sec < 30 then 1 else 0 end)`,
- 'bitrate_2.4_3.4_watch_30', `sum(case when avg_bitrate_mbps >= 2.4 and avg_bitrate_mbps < 3.4 and watch_time_sec >= 30 then 1 else 0 end)`,
- 'bitrate_3.4_watch_1', `sum(case when avg_bitrate_mbps >= 3.4 and watch_time_sec > 0 and watch_time_sec < 1 then 1 else 0 end)`,
- 'bitrate_3.4_watch_1_15', `sum(case when avg_bitrate_mbps >= 3.4 and watch_time_sec >= 1 and watch_time_sec < 15 then 1 else 0 end)`,
- 'bitrate_3.4_watch_15_30', `sum(case when avg_bitrate_mbps >= 3.4 and watch_time_sec >= 15 and watch_time_sec < 30 then 1 else 0 end)`,
- 'bitrate_3.4_watch_30', `sum(case when avg_bitrate_mbps >= 3.4 and watch_time_sec >= 30 then 1 else 0 end)`