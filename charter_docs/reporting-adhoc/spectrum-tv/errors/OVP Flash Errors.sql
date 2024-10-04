//OVP Active Devices for a month

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

select
count(distinct visit__device__uuid)
from prod.venona_events
WHERE
((message__name = 'loginStop' AND operation__success = TRUE) OR message__name = 'playbackHeartbeat')
 and visit__application_details__application_type = 'OVP'
 and partition_date_utc >= '2017-06-01'
 and partition_date_utc <= '2017-06-30'

 ---------------------------------------------

//Devices that saw the Flash error on OVP

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

select 
month_utc,
count(UUID)
from
(
select 
substr(partition_date_utc,0,7) as month_utc,
visit__device__uuid as UUID,
sum(if(message__name = 'pageView',1,0)) as flashErrors,
from prod.venona_events
where partition_date_utc >= "2017-06-01"
and partition_date_utc <= "2017-06-30"
and visit__application_details__application_type = "OVP" 
and ((state__view__current_page__page_name in ('adobeFlashNotAvailableWarning', 'adobeFlashUpgradeWarning')
and message__name = "pageView"))
group by 
substr(partition_date_utc,0,7),
visit__device__uuid
) a
where 
flashErrors > 0
group by
month_utc

---------------------------------------------

//Devices that saw the flash error and did not stream on OVP

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

select 
month_utc,
count(UUID)
from
(
select 
substr(partition_date_utc,0,7) as month_utc,
visit__device__uuid as UUID,
sum(if(message__name = 'pageView',1,0)) as flashErrors,
sum(if(message__name = 'playbackStart', 1,0)) playbackStarts
from prod.venona_events
where partition_date_utc >= "2017-06-01"
and partition_date_utc <= "2017-06-30"
and visit__application_details__application_type = "OVP" 
and ((state__view__current_page__page_name in ('adobeFlashNotAvailableWarning', 'adobeFlashUpgradeWarning')
and message__name = "pageView") OR message__name = "playbackStart")
group by 
substr(partition_date_utc,0,7),
visit__device__uuid
) a
where 
flashErrors > 0
and playbackStarts = 0 
group by
month_utc


