SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;

--SELECT
--customer_type
--,avg(dfcr) as 6_mo_avg_dfcr 
--FROM (
SELECT 
fiscal_month
, cd.customer_type
, sum(calls_with_visit) as calls_with_visit
, sum(validated_calls) as validated_calls
, sum(calls_with_visit)/sum(validated_calls) as dfcr

FROM (
	SELECT c.customer_type
	, c.call_end_date_utc
	, m.fiscal_month
	, count(distinct (case when lower(dev.aes_decrypt256(c.account_number)) !='unknown' AND c.enhanced_account_number = 0 then c.call_inbound_key end)) as validated_calls
	FROM 	
	  prod.cs_call_care_data c
	  JOIN prod_lkp.chtr_fiscal_month m ON c.call_end_date_utc = m.partition_date
	WHERE call_end_date_utc >="2018-12-22" AND call_end_date_utc<'2019-09-29' and segment_handled_flag=1
	GROUP BY c.customer_type, m.fiscal_month, c.call_end_date_utc
	) cd
JOIN 
	(SELECT call_date
	, customer_type
	, sum(total_visits) as authenticated_visits
	, sum(calls_with_visit) as calls_with_visit
	FROM prod.cs_call_in_rate
	WHERE call_date >='2018-12-22' AND call_date <'2019-09-29' AND customer_type<>'UNMAPPED'
	GROUP BY call_date, customer_type
	) vd on vd.call_date = cd.call_end_date_utc AND vd.customer_type = cd.customer_type
GROUP BY fiscal_month, cd.customer_type
ORDER BY customer_type,fiscal_month
--) months
--GROUP BY customer_type
;

--  I believe I can combine this with CIR once I'm inserting into tables, because 
-- I can use the FROM.... SELECT when inserting


