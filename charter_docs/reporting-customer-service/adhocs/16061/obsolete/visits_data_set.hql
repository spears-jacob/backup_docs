SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true;

SELECT 
visit__visit_id as visit_id
 , visit__account__account_number
 , next_click
 , CASE WHEN next_click in ('cancelIssue','resetEquip') then 'troubleshooting flow'
	WHEN next_click in ('manualResetContinue','manualResetClose') then 'manual reset'
	ELSE 'other' END as customer_category
 , received__timestamp as visit_time
FROM 
	(SELECT 
	 visit__visit_id,
	 message__name,
	 state__view__current_page__page_name,
	 state__view__current_page__elements__standardized_name,
	 visit__account__account_number,
	 cast(received__timestamp/1000 as BIGINT) as received__timestamp,
	 LEAD(state__view__current_page__page_name,1) OVER w AS next_page,	
	 LEAD(state__view__current_page__elements__standardized_name,1) OVER w AS next_click
	FROM 
		prod.asp_v_venona_events_portals_msa
	WHERE partition_date_utc>='2019-06-07' 
		AND message__name in ('selectAction') 
--		AND visit__account__account_number is not null                                -- Pull only authenticated visits
--		AND visit__account__account_number != 'GSWNkZXIfDPD6x25Na3i8g==' -- remove pending account numbers
  --		AND visit__account__account_number != '7FbKtybuOWU4/Q0SRInbHA==' -- remove empty string account numbers
  --		AND visit__visit_id is not null
	WINDOW w as (PARTITION BY visit__visit_id ORDER BY received__timestamp, message__timestamp
	)) sub
WHERE
 state__view__current_page__page_name='internetTab'
 AND state__view__current_page__elements__standardized_name='troubleshoot'
--	   AND visit__visit_id='11df4c7a-38b8-4b54-b828-a5f69aee2302'
	 AND next_click in ('manualResetContinue','manualResetClose', 'resetEquip','cancelIssue')
limit 10
;



