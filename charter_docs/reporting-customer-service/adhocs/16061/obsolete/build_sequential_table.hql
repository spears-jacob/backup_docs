SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

DROP VIEW if exists dev_tmp.cs_venona_sequential_events_20190610;

CREATE VIEW dev_tmp.cs_venona_sequential_events_20190610 AS
SELECT  
first.visit__visit_id as visit_id
, first.message__name as first_message
, first.state__view__current_page__page_name as first_page
, first.state__view__current_page__elements__standardized_name as first_action
, second.message__name as second_message
, second.state__view__current_page__elements__standardized_name as second_action
, second.received__timestamp - first.received__timestamp as time_differential
FROM
(SELECT 
state__view__current_page__page_name
	, message__name
	, state__view__current_page__elements__standardized_name
	, received__timestamp
	,visit__visit_id --, partition_date_hour_utc
FROM prod.asp_v_venona_events_portals_msa
WHERE 
--	AND state__view__current_page__elements__standardized_name='troubleshoot'
--	AND state__view__current_page__page_name='internetTab' AND
	partition_date_hour_utc>='2019-06-07'
	AND message__name in ('selectAction','pageView')
--	AND visit__visit_id='32d72e7d-0309-4e0c-a26c-e3054ad123a7'
) first
JOIN
(SELECT 
state__view__current_page__page_name
	, message__name
	, state__view__current_page__elements__standardized_name
	, received__timestamp
	,visit__visit_id --, partition_date_hour_utc
FROM prod.asp_v_venona_events_portals_msa
WHERE 
--	AND state__view__current_page__elements__standardized_name='troubleshoot'
--	AND state__view__current_page__page_name='internetTab' AND
	partition_date_hour_utc>='2019-06-07'
	AND message__name in ('selectAction','pageView')
--	AND visit__visit_id='32d72e7d-0309-4e0c-a26c-e3054ad123a7'
) second
on first.visit__visit_id = second.visit__visit_id
WHERE 
 first.state__view__current_page__elements__standardized_name='troubleshoot'
	AND first.state__view__current_page__page_name='internetTab'
	AND first.received__timestamp < second.received__timestamp
ORDER BY time_differential
;

DROP VIEW if exists dev_tmp.cs_sequential_next_action_20190610;
CREATE VIEW dev_tmp.cs_sequential_next_action_20190610 AS
SELECT visit_id
, min(abs(time_differential)) as min_time_differential
FROM  
dev_tmp.cs_venona_sequential_events_20190610
GROUP BY visit_id
;

SELECT * FROM
dev_tmp.cs_sequential_next_action_20190610 min
INNER JOIN dev_tmp.cs_venona_sequential_events_20190610 ev
ON ev.visit_id=min.visit_id AND ev.time_differential=min.min_time_differential
WHERE second_action!='resetEquip'
limit 10;
