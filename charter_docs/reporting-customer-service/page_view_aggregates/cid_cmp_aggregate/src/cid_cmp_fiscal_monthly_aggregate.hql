--Update CID fiscal monthly aggregate table
INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_monthly_quantum_pageview_cid_aggregate PARTITION (fiscal_month)
SELECT CASE WHEN application_name IS NOT NULL OR application_name <> '' THEN application_name ELSE 'Unknown' END application_name
,CASE WHEN application_api_host IS NOT NULL OR application_api_host <> '' THEN application_api_host ELSE 'Unknown' END application_api_host
,CASE WHEN URL_new IS NOT NULL OR URL_new <> '' THEN URL_new ELSE 'Unknown' END URL
,CASE WHEN CID IS NOT NULL or CID <> '' THEN CID ELSE 'Unknown' END CID
,message_name
,COUNT(1) count_of_pageviews
,COUNT(DISTINCT visit_device_uuid) count_of_unique_users --count_of_unique_visitors
,COUNT(visit_device_uuid) count_of_users --count_of_visitors
,COUNT(DISTINCT unique_visit_id) count_of_visits
,fiscal_month
FROM ${env:ENVIRONMENT}.cs_quantum_cid_pageviews cid
  INNER JOIN ${env:LKP_db}.chtr_fiscal_month dates
    ON cid.partition_date_utc = dates.partition_date
WHERE lower(message_name) = 'pageview'
GROUP BY CASE WHEN application_name IS NOT NULL OR application_name <> '' THEN application_name ELSE 'Unknown' END
,CASE WHEN application_api_host IS NOT NULL OR application_api_host <> '' THEN application_api_host ELSE 'Unknown' END
,CASE WHEN URL_new IS NOT NULL OR URL_new <> '' THEN URL_new ELSE 'Unknown' END
,CASE WHEN CID IS NOT NULL or CID <> '' THEN CID ELSE 'Unknown' END
,message_name
,fiscal_month
;

--Update CMP fiscal monthly aggregate table
INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.cs_monthly_quantum_pageview_cmp_aggregate PARTITION (fiscal_month)
SELECT CASE WHEN application_name IS NOT NULL OR application_name <> '' THEN application_name ELSE 'Unknown' END application_name
,CASE WHEN application_api_host IS NOT NULL OR application_api_host <> '' THEN application_api_host ELSE 'Unknown' END application_api_host
,CASE WHEN URL_new IS NOT NULL OR URL_new <> '' THEN URL_new ELSE 'Unknown' END URL
,CASE WHEN CMP IS NOT NULL or CMP <> '' THEN CMP ELSE 'Unknown' END CMP
,message_name
,COUNT(1) count_of_pageviews
,COUNT(DISTINCT visit_device_uuid) count_of_unique_users --count_of_unique_visitors
,COUNT(visit_device_uuid) count_of_users --count_of_visitors
,COUNT(DISTINCT unique_visit_id) count_of_visits
,fiscal_month
FROM ${env:ENVIRONMENT}.cs_quantum_cid_pageviews cid
  INNER JOIN ${env:LKP_db}.chtr_fiscal_month dates
    ON cid.partition_date_utc = dates.partition_date
WHERE lower(message_name) = 'pageview'
GROUP BY CASE WHEN application_name IS NOT NULL OR application_name <> '' THEN application_name ELSE 'Unknown' END
,CASE WHEN application_api_host IS NOT NULL OR application_api_host <> '' THEN application_api_host ELSE 'Unknown' END
,CASE WHEN URL_new IS NOT NULL OR URL_new <> '' THEN URL_new ELSE 'Unknown' END
,CASE WHEN CMP IS NOT NULL or CMP <> '' THEN CMP ELSE 'Unknown' END
,message_name
,fiscal_month
;
