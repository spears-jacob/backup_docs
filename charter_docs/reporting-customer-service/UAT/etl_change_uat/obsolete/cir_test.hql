SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;

--SELECT cd.*
--	, cir.total_acct_calls
--	, cir.total_calls
--	, cir.handled_acct_calls
--      , "Discrepancy" as should_be_empty
--FROM (
--SELECT call_end_date_utc as call_date,
--                  agent_mso,
--                visit_type
--                  customer_type,
--                  count(distinct (CASE WHEN enhanced_account_number = 0 THEN account_number END)) as generated_total_acct_calls,
--                  count(distinct call_inbound_key) as generated_total_calls,
--                  count(distinct (case when lower(dev.aes_decrypt256(account_number)) !='unknown' AND enhanced_account_number = 0 then call_inbound_key end)) as generated_handled_acct_calls
--            FROM test.cs_call_care_data_amo_julyv3
--            WHERE segment_handled_flag = true
--          GROUP BY call_end_date_utc, agent_mso, customer_type) cd
--join test.cs_call_in_rate_amo_julyv3 cir
--        on cir.call_date=cd.call_date
--        AND cir.agent_mso=cd.agent_mso
--        AND cir.customer_type=cd.customer_type
--WHERE 100*(generated_total_acct_calls-total_acct_calls)/total_acct_calls > 4
--limit 10-
--;



SELECT DISTINCT account_number 
FROM test.cs_call_care_data_amo_julyv3
WHERE enhanced_account_number=0
AND segment_handled_flag=1
AND call_end_date_utc='2019-07-08'
AND agent_MSO='TWC'
AND customer_type='RESIDENTIAL'
