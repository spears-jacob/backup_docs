set hive.resultset.use.unique.column.names=false;

INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_prod_monthly_fiscal_month_metrics PARTITION (fiscal_month = '${hiveconf:FISCAL_MONTH}')
SELECT
 "cir" as metric
, "COMBINED" as customer_type
, sum(calls_with_visit)/sum(authenticated_visits) as value

FROM
			(SELECT call_date
				, sum(total_visits) as authenticated_visits
				, sum(calls_with_visit) as calls_with_visit
				FROM prod.cs_call_in_rate
				WHERE call_date >="${hiveconf:subquery_start}"
        AND call_date <=DATE_ADD("${hiveconf:subquery_start}",41)
				AND customer_type<>'UNMAPPED'
				GROUP BY call_date
				) vd
JOIN prod_lkp.chtr_fiscal_month m
on vd.call_date = m.partition_date
WHERE m.fiscal_month="${hiveconf:FISCAL_MONTH}"
GROUP BY fiscal_month
;

INSERT INTO TABLE ${env:ENVIRONMENT}.cs_prod_monthly_fiscal_month_metrics PARTITION (fiscal_month = '${hiveconf:FISCAL_MONTH}')
SELECT
 "dfcr" as metric
, cd.customer_type as customer_type
, sum(calls_with_visit)/sum(validated_calls) as value
FROM (
				SELECT c.customer_type
				, c.call_end_date_utc
				, m.fiscal_month
        , count(distinct (case when c.encrypted_padded_account_number_256!='+yzQ1eS5iRmWnflCmvNlSg==' AND c.enhanced_account_number = 0 then c.call_inbound_key end)) as validated_calls
				FROM
          prod.red_cs_call_care_data_v c
				JOIN prod_lkp.chtr_fiscal_month m ON c.call_end_date_utc = m.partition_date
				WHERE call_end_date_utc >="${hiveconf:subquery_start}"
        AND call_end_date_utc <=DATE_ADD("${hiveconf:subquery_start}",41)
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
        AND call_date <=DATE_ADD("${hiveconf:subquery_start}",41)
				AND customer_type<>'UNMAPPED'
				GROUP BY call_date, customer_type
				) vd on vd.call_date = cd.call_end_date_utc AND vd.customer_type = cd.customer_type
GROUP BY fiscal_month, cd.customer_type
;
