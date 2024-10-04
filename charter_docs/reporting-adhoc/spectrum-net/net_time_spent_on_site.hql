select
	CASE
		WHEN a.HH_id is null then 'Unauthenticated'
		ELSE 'Authenticated'
	END as HH_authentication,
	SUM(a.visit_duration_sec) as total_time_on_site_sec,
	SUM(a.visit_duration_sec)/60 as total_time_on_site_min,
	SUM(a.visit_duration_sec)/3600 as total_time_on_site_hr,
	(SUM(a.visit_duration_sec)/60)/COUNT(a.HH_id) as time_on_site_min_per_HH
from
	(
	select
		partition_date,
		COALESCE(visit__account__account_number) as HH_id,
		visit__visit_id as visit_id,
		--MIN(message__timestamp) as visit_start,
		--MAX(message__timestamp) as visit_end,
		MAX(message__timestamp) - MIN(message__timestamp) as visit_duration_sec
	from prod.net_events
	where 
		partition_date = '2017-03-01'
	group by
		partition_date,
		visit__account__account_number,
		visit__visit_id
	having
		(MAX(message__timestamp) - MIN(message__timestamp) > 0)
	) a
group by 
	CASE
		WHEN a.HH_id is null then 'Unauthenticated'
		ELSE 'Authenticated'
	END