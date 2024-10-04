set hive.resultset.use.unique.column.names=false;

	SELECT
	 sum(calls_with_visit)/sum(authenticated_visits) as value
	, "cir" as metric
	, "COMBINED" as customer_type
	FROM
		(SELECT call_date
		, sum(total_visits) as authenticated_visits
		, sum(calls_with_visit) as calls_with_visit
		FROM prod.cs_call_in_rate
		WHERE call_date >="${hiveconf:start_date}"
		AND call_date <"${hiveconf:end_date}"
		AND customer_type<>'UNMAPPED'
		GROUP BY call_date
		) vd
	JOIN prod_lkp.chtr_fiscal_month m
	on vd.call_date = m.partition_date
	GROUP BY fiscal_month
;

	SELECT
	 sum(calls_with_visit)/sum(authenticated_visits) as value
	, "cir" as metric
	, "COMBINED" as customer_type
	FROM
		(SELECT call_date
		, sum(total_visits) as authenticated_visits
		, sum(calls_with_visit) as calls_with_visit
		FROM prod.cs_call_in_rate
		WHERE call_date >="${hiveconf:subquery_start}"
		AND customer_type<>'UNMAPPED'
		GROUP BY call_date
		) vd
	JOIN prod_lkp.chtr_fiscal_month m
	on vd.call_date = m.partition_date
	WHERE m.fiscal_month="${hiveconf:FISCAL_MONTH}"
	GROUP BY fiscal_month
;

