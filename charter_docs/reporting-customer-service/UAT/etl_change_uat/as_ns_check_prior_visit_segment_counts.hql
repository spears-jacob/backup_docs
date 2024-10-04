SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;

--Assumption: Regardless of what else we've changed, the number of _handled_ segments for each call should be the same between
----- the call_care_data table and the calls_with_prior_visits table.
--All Segments Included (as)
------ Same number of handled segments (as.ns)

--discrepancy should be 0 for every call_inbound_key/customer_type pair

SELECT * FROM(
SELECT
ccd.call_inbound_key
, ccd.customer_type
, visit_type
, call_segment_count
, cwv_segment_count
, (call_segment_count - cwv_segment_count) as discrepancy
, "this should return 0 records" as should_return_0_records
FROM (
	--count of rows by calls in call_care_data
		--customer type because there are duplicates with different types
	SELECT
	CALL_inbound_key
	, customer_type
	, count(distinct segment_id) as call_segment_count
	FROM
	 ${hiveconf:new_call_table}
	WHERE
	  segment_handled_flag=1
	--  AND call_end_date_utc>='2019-07-09'
	--  AND call_end_date_utc<='2019-07-12'
	GROUP BY call_inbound_key,customer_type
	) ccd
INNER JOIN (
	--count of rows by call in calls_with_prior_visits
		--cusotmer type is ncessary because there are duplicate with different types
	SELECT
	 call_inbound_key
	 , visit_type
	 , customer_type
	 , count(call_date) as cwv_segment_count
	FROM
	 ${hiveconf:new_visits_table}
--	WHERE
--	  call_date >="2019-07-09"
--	  AND call_date<"2019-07-12"
	GROUP BY call_inbound_key, customer_type, visit_type
	) cwpv
 ON ccd.call_inbound_key=cwpv.call_inbound_key AND ccd.customer_type=cwpv.customer_type ) disc
WHERE discrepancy<>0
limit 10
;
