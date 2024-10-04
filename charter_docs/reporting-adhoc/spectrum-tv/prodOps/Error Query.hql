select
message__timestamp,
message__name,
application__error__error_type,
application__error__error_code,
application__error__error_message,
application__error__error_extras
from prod.venona_events
where partition_date_utc >= '2017-05-10' 
and visit__device__uuid = '8d1f8a6e05265d90'
and message__name in ('error','playbackFailure')