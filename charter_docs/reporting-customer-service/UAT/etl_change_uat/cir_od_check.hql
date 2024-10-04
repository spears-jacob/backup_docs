SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--Assumption: the call-in rate and digital-first-contact rates should not change
--- much from what they were before
--Call-in Rate (cir)
------ Matches old data (cir_od)

-- it pulls anything where they're not exactly equal; you have to exercise
--- your judgement about whether that's an OK outcome or if more research needs
--- to be done

SELECT avg(discrepancy) as is_this_acceptable_CIR_percentage_point_deviance FROM (
SELECT
old.*
, new.CIR as new_cir
, new.CIR-old.CIR as discrepancy
	FROM (
	SELECT
	 call_date
	 , agent_mso
	 , visit_type
	 , customer_type
	 , 100*(calls_with_visit/total_visits) as CIR
	FROM
	  ${hiveconf:old_cir_table}) old
	JOIN (
	SELECT
	call_date
	, agent_mso
	, visit_type
	, customer_type
	, 100*(calls_with_visit/total_visits) as CIR
	FROM --test.cs_call_in_rate_amo_julyv3) new
	  ${hiveconf:new_cir_table}) new
	ON new.call_date=old.call_date AND old.agent_mso=new.agent_mso AND old.customer_type=new.customer_type
WHERE old.CIR<>new.CIR) av
limit 10
;

SELECT --* -- uncomment this to make it research
 avg(discrepancy) as is_this_acceptable_dfcr_percentage_point_deviance
FROM (
SELECT
old.*
--,new.*
, new.dfcr as new_dfcr
, new.dfcr-old.dfcr as discrepancy
	FROM (
	SELECT
	 call_date
	 , agent_mso
	 , visit_type
	 , customer_type
	 , calls_with_visit
	 , handled_acct_calls
	 , 100*(calls_with_visit/handled_acct_calls) as dfcr
	FROM
	--prod.cs_call_in_rate) old
	  ${hiveconf:old_cir_table}) old
	JOIN (
	SELECT
	call_date
	, agent_mso
	, visit_type
	, customer_type
        , calls_with_visit
	, handled_acct_calls
	, 100*(calls_with_visit/handled_acct_calls) as dfcr
	FROM
	--test.cs_call_in_rate_amo_julyv3) new
	  ${hiveconf:new_cir_table}) new
	ON new.call_date=old.call_date AND old.agent_mso=new.agent_mso AND old.customer_type=new.customer_type
WHERE old.dfcr<>new.dfcr) av
limit 10
;
