SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

DROP VIEW if exists dev_tmp.cs_experiencing_issues_calls_20190618;
DROP VIEW if exists dev_tmp.cs_experiencing_issues_calls;

CREATE VIEW dev_tmp.cs_experiencing_issues_calls AS
SELECT *, 
 call_start_timestamp_utc/1000 - visit_time as time_to_call,
 if(call_start_timestamp_utc/1000 - visit_time BETWEEN 0 and 18600,1,0) as call_flag 
FROM (
  SELECT
  visit__visit_id as visit_id
   , visit__account__account_number as vis_account_number
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
          WHERE partition_date_utc>="${hiveconf:start_date}"
                  AND message__name in ('selectAction')
          WINDOW w as (PARTITION BY visit__visit_id ORDER BY received__timestamp, message__timestamp
          )) subsub
  WHERE
   state__view__current_page__page_name='internetTab'
   AND state__view__current_page__elements__standardized_name='troubleshoot'
  --         AND visit__visit_id='11df4c7a-38b8-4b54-b828-a5f69aee2302'
           AND next_click in ('manualResetContinue','manualResetClose', 'resetEquip','cancelIssue')
   ) visits
JOIN (SELECT DISTINCT call_inbound_key, call_start_timestamp_utc, account_number as call_account_number 
	FROM prod.cs_call_data WHERE call_end_date_utc>="${hiveconf:start_date}") calls  
	on prod.aes_decrypt256(call_account_number)=prod.aes_decrypt(vis_account_number)
--limit 10
  

