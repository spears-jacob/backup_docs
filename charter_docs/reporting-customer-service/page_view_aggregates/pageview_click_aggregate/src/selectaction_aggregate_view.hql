DROP VIEW if exists ${env:ENVIRONMENT}.cs_quantum_pageview_selectaction_aggregates;
CREATE VIEW ${env:ENVIRONMENT}.cs_quantum_pageview_selectaction_aggregates AS 

SELECT  application_name,
		current_page,
		element_name,
		count_of_buttonclicks,
		count_of_unique_visitors,
		count_of_visitors,
		count_of_distinct_visits, 
		to_date(partition_date_utc) as day, 
		"daily" as grain
FROM ${env:ENVIRONMENT}.cs_daily_pageview_selectaction_aggregate

UNION ALL

SELECT  application_name,
		current_page,
		element_name,
		count_of_buttonclicks,
		count_of_unique_visitors,
		count_of_visitors,
		count_of_distinct_visits,
		week_starting as day, 
		"weekly" as grain
FROM ${env:ENVIRONMENT}.cs_weekly_pageview_selectaction_aggregate

UNION ALL

SELECT  application_name,
		current_page,
		element_name,
		count_of_buttonclicks,
		count_of_unique_visitors,
		count_of_visitors,
		count_of_distinct_visits, 
		To_Date(CONCAT(fiscal_month,'-01')) as day,
		"fiscal monthly" as grain
FROM ${env:ENVIRONMENT}.cs_monthly_pageview_selectaction_aggregate

UNION ALL

SELECT  application_name,
		current_page,
		element_name,
		count_of_buttonclicks,
		count_of_unique_visitors,
		count_of_visitors,
		count_of_distinct_visits,  
		To_Date(CONCAT(calendar_month,'-01')) as day,
		"calendar monthly" as grain
FROM ${env:ENVIRONMENT}.cs_calendar_monthly_pageview_selectaction_aggregate
;