-- we're trying to understand why we only see the "highest" bitrate for Roku a very small percentage of the time. I was hoping you could run a report with "group by Connection type, ISP, Application version, Device version, bitrate" with raw counts of streams / views for each breakdown for us to analyze patterns.

with base_data as (
  select
    state__content__stream__playback_id as stream_id,
    max(visit__application_details__application_type) as application_type,
    max(visit__connection__connection_type) as connection_type,
    max(visit__isp__isp) as isp,
    max(visit__application_details__app_version) as application_version,
    max(visit__device__model) as device_model,
    SUM((COALESCE(state__playback__bitrate__current_bitrate_mbps, 0) * COALESCE(state__playback__bitrate__content_elapsed_at_current_bitrate_ms, 0) / 1000)
    + (COALESCE(state__playback__bitrate__previous_bitrate_mbps, 0) * COALESCE(state__playback__bitrate__content_elapsed_at_previous_bitrate_ms, 0) / 1000))
    / SUM(COALESCE(state__playback__bitrate__content_elapsed_at_current_bitrate_ms, 0) / 1000
    + COALESCE(state__playback__bitrate__content_elapsed_at_previous_bitrate_ms, 0) / 1000) AS avg_bitrate_mbps
  from prod.venona_events
  where
    ((partition_date = '2017-04-14' and hour >= '06')
      and (partition_date between '2017-04-15' and '2017-04-17')
      or  (partition_date = '2017-04-18' and hour < '06'))
    and state__content__stream__playback_id <> ''
    and isnotnull(state__content__stream__playback_id)
  group by state__content__stream__playback_id
)
SELECT
  application_type,
  connection_type,
  isp,
  application_version,
  device_model,
  CAST(hist.x as double) as bin_center,
  CAST(hist.y as int) as bin_height
FROM (
  select
    application_type,
    connection_type,
    isp,
    application_version,
    device_model,
    histogram_numeric(avg_bitrate_mbps, 10) as histogram
  from base_data
  where application_type <> 'OVP'
  -- and isp in ('Time Warner Cable', 'Charter Communications')
  group by
    application_type,
    connection_type,
    isp,
    application_version,
    device_model
  ) a
LATERAL VIEW explode(histogram) exploded_table as hist;
