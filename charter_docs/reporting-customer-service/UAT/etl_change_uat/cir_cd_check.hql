SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;

--Assumption: Regardless of what else we've changed, call-in rate should make sense
-- given the data in the other tables
--Call-in Rate (cir)
------ Consistent data between tables (cir_cd)

--Should return 0 rows

SELECT cwv.*
	, cir.calls_with_visit
        , "Discrepancy" as should_be_empty
FROM (
      	SELECT
	call_date,
        agent_mso,
       customer_type,
        visit_type,
        count(distinct call_inbound_key) as generated_calls_with_visit
FROM ${hiveconf:new_visits_table} cwv
GROUP BY call_date,agent_mso,customer_type,visit_type
) cwv
join ${hiveconf:new_cir_table} cir
        on cir.call_date=cwv.call_date
        AND cir.agent_mso=cwv.agent_mso
        AND cir.customer_type=cwv.customer_type
        AND UPPER(cir.visit_type)=UPPER(cwv.visit_type)
WHERE generated_calls_with_visit<>calls_with_visit
;

SELECT cd.*
	, cir.total_acct_calls
	, total_calls
	, handled_acct_calls
	, "Discrepancy" as should_be_empty
FROM ( 
SELECT call_end_date_utc as call_date,
                  account_agent_mso,
                  customer_type,
                  count(distinct (CASE WHEN enhanced_account_number = 0 THEN account_number END)) as generated_total_acct_calls,
                  count(distinct call_inbound_key) as generated_total_calls,
                  count(distinct (case when lower(dev.aes_decrypt256(account_number)) !='unknown' AND enhanced_account_number = 0 then call_inbound_key end)) as generated_handled_acct_calls
	    FROM ${hiveconf:new_call_table}
	     WHERE segment_handled_flag = true
           GROUP BY call_end_date_utc, account_agent_mso, customer_type) cd
join ${hiveconf:new_cir_table} cir
	on cir.call_date=cd.call_date 
	AND cir.agent_mso=cd.account_agent_mso 
	AND cir.customer_type=cd.customer_type
WHERE
	total_acct_calls<>generated_total_acct_calls OR
	total_calls<>generated_total_calls OR
	handled_acct_calls<>generated_handled_acct_calls
--limit 10
;


