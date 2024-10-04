#### This query was run for the UX team, as they were asking for usage of the companion features (VOD Flick to Stream and STB Device Rename) from Spectrum TV

SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
select 
substr(from_utc_timestamp(to_utc_timestamp(received__timestamp, 'America/Chicago'), 'America/Denver'),0,10) as denver_date_time,
visit__application_details__application_type,
application__api__api_name,
count(distinct visit__device__uuid) as count_devices,
count(*) as count_events
from prod.venona_events
where 
partition_date_utc >= '2017-04-01'
and application__api__api_name in ('vodFlick', 'stbName')
group by
substr(from_utc_timestamp(to_utc_timestamp(received__timestamp, 'America/Chicago'), 'America/Denver'),0,10),
visit__application_details__application_type,
application__api__api_name
;