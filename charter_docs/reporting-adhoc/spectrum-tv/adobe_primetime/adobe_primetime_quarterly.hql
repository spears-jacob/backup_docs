--distinct devices by app_type

"SELECT '2017-02' AS year_month,
       visit__application_details__application_type AS app_type,
       size(collect_Set(visit__device__uuid)) AS unique_devices
FROM prod.venona_events
WHERE DATE(from_utc_timestamp(concat(partition_date, ' ', lpad(hour, 2, '0'), ':00:00'), 'America/Denver')) BETWEEN '2017-02-01' AND '2017-02-28'
  AND message__name = 'playbackStart'
GROUP BY visit__application_details__application_type;"					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
					
