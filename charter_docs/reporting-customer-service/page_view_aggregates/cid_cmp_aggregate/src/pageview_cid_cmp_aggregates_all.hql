DROP VIEW if exists ${env:ENVIRONMENT}.cs_quantum_pageview_cid_aggregates;
CREATE VIEW ${env:ENVIRONMENT}.cs_quantum_pageview_cid_aggregates 
AS 

SELECT application_api_host, 
       	 application_name, 
 cid, 
 count_of_events as count_of_pageviews,      
 count_of_unique_users,
 count_of_users, 
 count_of_visits, 
 message_name, 
 url, 
 to_date(partition_date_utc) as day,
 "daily" as grain
FROM ${env:ENVIRONMENT}.cs_daily_quantum_pageview_cid_aggregate
WHERE message_name = "pageView"

UNION ALL

SELECT application_api_host, 
       	 application_name, 
 cid, 
 count_of_pageviews,      
 count_of_unique_users,
 count_of_users, 
 count_of_visits, 
 message_name, 
 url, 
 week_starting as day, 
 "weekly" as grain
FROM ${env:ENVIRONMENT}.cs_weekly_quantum_pageview_cid_aggregate
WHERE message_name = "pageView"

UNION ALL

SELECT application_api_host, 
       	 application_name, 
 cid, 
 count_of_pageviews,      
 count_of_unique_users,
 count_of_users, 
 count_of_visits, 
 message_name, 
 url,
To_Date(CONCAT(fiscal_month,'-01')) as day, 
"fiscal monthly" as grain
FROM ${env:ENVIRONMENT}.cs_monthly_quantum_pageview_cid_aggregate
WHERE message_name = "pageView"

UNION ALL

SELECT application_api_host, 
       	 application_name, 
 cid, 
 count_of_pageviews,      
 count_of_unique_users,
 count_of_users, 
 count_of_visits, 
 message_name, 
 url,
To_Date(CONCAT(calendar_month,'-01')) as day, 
"calendar monthly" as grain
FROM ${env:ENVIRONMENT}.cs_calendar_monthly_quantum_pageview_cid_aggregate
WHERE message_name = "pageView"
;


DROP VIEW if exists ${env:ENVIRONMENT}.cs_quantum_pageview_cmp_aggregates;
CREATE VIEW ${env:ENVIRONMENT}.cs_quantum_pageview_cmp_aggregates 
AS 

SELECT application_api_host, 
       	 application_name, 
 cmp, 
 count_of_events as count_of_pageviews,      
 count_of_unique_users,
 count_of_users, 
 count_of_visits, 
 message_name, 
 url, 
 to_date(partition_date_utc) as day,
 "daily" as grain
FROM ${env:ENVIRONMENT}.cs_daily_quantum_pageview_cmp_aggregate
WHERE message_name = "pageView"

UNION ALL

SELECT application_api_host, 
       	 application_name, 
 cmp, 
 count_of_pageviews,      
 count_of_unique_users,
 count_of_users, 
 count_of_visits, 
 message_name, 
 url, 
 week_starting as day, 
 "weekly" as grain
FROM ${env:ENVIRONMENT}.cs_weekly_quantum_pageview_cmp_aggregate
WHERE message_name = "pageView"

UNION ALL

SELECT application_api_host, 
       	 application_name, 
 cmp, 
 count_of_pageviews,      
 count_of_unique_users,
 count_of_users, 
 count_of_visits, 
 message_name, 
 url,
To_Date(CONCAT(fiscal_month,'-01')) as day, 
"fiscal monthly" as grain
FROM ${env:ENVIRONMENT}.cs_monthly_quantum_pageview_cmp_aggregate
WHERE message_name = "pageView"

UNION ALL

SELECT application_api_host, 
       	 application_name, 
 cmp, 
 count_of_pageviews,      
 count_of_unique_users,
 count_of_users, 
 count_of_visits, 
 message_name, 
 url,
To_Date(CONCAT(calendar_month,'-01')) as day, 
"calendar monthly" as grain
FROM ${env:ENVIRONMENT}.cs_calendar_monthly_quantum_pageview_cmp_aggregate
WHERE message_name = "pageView"
;


