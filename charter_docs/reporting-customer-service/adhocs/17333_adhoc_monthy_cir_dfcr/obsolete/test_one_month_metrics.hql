set hive.resultset.use.unique.column.names=false;

	SELECT
	sum(calls_with_visit)/sum(validated_calls) as value
	, "dfcr" as metric
	, cd.customer_type as customer_type
	FROM (
		SELECT c.customer_type
		, c.call_end_date_utc
		, m.fiscal_month
		, count(distinct (case when lower(dev.aes_decrypt256(c.account_number)) !='unknown' AND c.enhanced_account_number = 0 then c.call_inbound_key end)) as validated_calls
		FROM
		  prod.cs_call_care_data c
	  	JOIN prod_lkp.chtr_fiscal_month m ON c.call_end_date_utc = m.partition_date
		WHERE call_end_date_utc >="${hiveconf:start_date}"
		AND call_end_date_utc<"${hiveconf:end_date}"
		and segment_handled_flag=1
		GROUP BY c.customer_type, m.fiscal_month, c.call_end_date_utc
		) cd
	JOIN (
		SELECT call_date
		, customer_type
		, sum(total_visits) as authenticated_visits
		, sum(calls_with_visit) as calls_with_visit
		FROM prod.cs_call_in_rate
		WHERE call_date >="${hiveconf:start_date}"
		AND call_date <"${hiveconf:end_date}"
		AND customer_type<>'UNMAPPED'
		GROUP BY call_date, customer_type
		) vd on vd.call_date = cd.call_end_date_utc AND vd.customer_type = cd.customer_type
	GROUP BY fiscal_month, cd.customer_type
;

	SELECT
	sum(calls_with_visit)/sum(validated_calls) as value
	, "dfcr" as metric
	, cd.customer_type as customer_type
	FROM (
		SELECT c.customer_type
		, c.call_end_date_utc
		, m.fiscal_month
		, count(distinct (case when lower(dev.aes_decrypt256(c.account_number)) !='unknown' AND c.enhanced_account_number = 0 then c.call_inbound_key end)) as validated_calls
		FROM
		  prod.cs_call_care_data c
	  	JOIN prod_lkp.chtr_fiscal_month m ON c.call_end_date_utc = m.partition_date
		WHERE call_end_date_utc >="${hiveconf:start_date}"
		AND m.fiscal_month = "${hiveconf:FISCAL_MONTH}"
		and segment_handled_flag=1
		GROUP BY c.customer_type, m.fiscal_month, c.call_end_date_utc
		) cd
	JOIN (
		SELECT call_date
		, customer_type
		, sum(total_visits) as authenticated_visits
		, sum(calls_with_visit) as calls_with_visit
		FROM prod.cs_call_in_rate
		WHERE call_date >="${hiveconf:subquery_start}"
		AND customer_type<>'UNMAPPED'
		GROUP BY call_date, customer_type
		) vd on vd.call_date = cd.call_end_date_utc AND vd.customer_type = cd.customer_type
	GROUP BY fiscal_month, cd.customer_type
;

