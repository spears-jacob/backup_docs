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

--SELECT avg(discrepancy) as is_this_acceptable_CIR_percentage_point_deviance FROM (
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
	  prod.cs_call_in_rate) old
	JOIN (
	SELECT
	call_date
	, agent_mso
	, visit_type
	, customer_type
	, 100*(calls_with_visit/total_visits) as CIR
	FROM --test.cs_call_in_rate_amo_julyv3) new
	  prod.cs_call_in_rate_corrected_20191204) new
	ON new.call_date=old.call_date AND old.agent_mso=new.agent_mso AND old.visit_type=new.visit_type
WHERE old.CIR<>new.CIR
AND old.call_date>='2019-11-01'
--) av
limit 10
;

