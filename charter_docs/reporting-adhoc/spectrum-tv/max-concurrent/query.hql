create external table if not exists test.venona_minute_all (
  partition_date string,
  hour string,
  min_time bigint,
  max_time bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/yxu/all';

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

select
  DATE(from_utc_timestamp(concat(partition_date, ' ', lpad(hour, 2, '0'), ':00:00'), 'America/Denver')) AS denver_date,
  application_type,
  playback_type,
  max(number_streams) as number_streams
from (
  select
    partition_date,
    hour,
    application_type,
    playback_type,
    min_time,
    size(collect_set(playback_id)) as number_streams
  from (
    select
      a.partition_date,
      a.hour,
      a.application_type,
      a.playback_type,
      a.playback_id,
      b.min_time,
      b.max_time
    from
      (
      select
        partition_date,
        hour,
        received__timestamp as received_timestamp,
        visit__application_details__application_type as application_type,
        state__content__stream__playback_type as playback_type,
        state__content__stream__playback_id as playback_id
      from
        prod.venona_events
      where
        ((partition_date = '2017-02-01' AND hour >= '07')
        OR (partition_date BETWEEN '2017-02-02' AND '2017-03-28')
        OR (partition_date = '2017-03-29' AND hour < '06'))
        and (
          isnotnull(visit__account__account_number)
          and isnotnull(visit__device__uuid)
          and isnotnull(visit__account__details__mso)
          and visit__application_details__application_type in ('iOS', 'Android', 'Roku', 'Xbox', 'OVP')
          and state__content__stream__playback_type in ('linear', 'vod')
          and message__category = 'playback'
          and visit__account__details__mso <> ''
          and TRIM(TRANSLATE(visit__device__uuid, '0', '')) <> ''
        )
      ) a
      join test.venona_minute_all b
      on
        a.partition_date = b.partition_date
        and a.hour = b.hour
      where a.received_timestamp > b.min_time and a.received_timestamp <= b.max_time
  ) join_hour
  group by
    partition_date,
    hour,
    application_type,
    playback_type,
    min_time
) hour_sample
group by
  DATE(from_utc_timestamp(concat(partition_date, ' ', lpad(hour, 2, '0'), ':00:00'), 'America/Denver')),
  application_type,
  playback_type
