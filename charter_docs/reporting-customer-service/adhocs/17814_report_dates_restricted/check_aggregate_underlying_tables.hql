SELECT max(fiscal_month)
,"monthly"
FROM prod.cs_monthly_pageview_selectaction_aggregate
WHERE fiscal_month >='2019-09-01'
;

SELECT partition_date_utc
,sum(count_of_visitors)
,"daily"
FROM prod.cs_daily_pageview_selectaction_aggregate
WHERE partition_date_utc >='2019-09-01'
GROUP BY partition_date_utc
;
