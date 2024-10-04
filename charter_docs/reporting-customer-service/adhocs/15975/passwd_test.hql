
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

SELECT 
	visit__visit_id as visit_id
	, max(if(state__view__current_page__elements__standardized_name='forgot-username-or-password',1,0)) as passwd_flag
	, max(if(state__view__current_page__page_name='verifyYourAccount',1,0)) as cpni_flag
FROM prod.asp_v_venona_events_portals_specnet
WHERE
	partition_date_utc>='2019-06-07'
GROUP BY visit__visit_id

limit 10;
