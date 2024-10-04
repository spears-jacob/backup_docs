SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;

--SELECT 
--avg(call_in_rate) as 6_mo_avg_cir
--FROM (
SELECT fiscal_month
, sum(calls_with_visit) calls_with_visit
, sum(authenticated_visits) as authenticated_visits
, sum(calls_with_visit)/sum(authenticated_visits) as call_in_rate
FROM
(SELECT call_date
, sum(total_visits) as authenticated_visits
, sum(calls_with_visit) as calls_with_visit
FROM prod.cs_call_in_rate
WHERE call_date >='2018-12-22' AND call_date <'2019-09-29' AND customer_type<>'UNMAPPED'
GROUP BY call_date
) vd 
JOIN prod_lkp.chtr_fiscal_month m
on vd.call_date = m.partition_date
GROUP BY fiscal_month
--) months

;



