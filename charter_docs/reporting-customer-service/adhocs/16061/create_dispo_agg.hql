SELECT 
customer_category
, issue_description
, cause_description
, resolution_description
,count(distinct visit_id)
FROM 
dev_tmp.cs_16061_ready_to_agg
WHERE call_flag=1
GROUP BY 
customer_category
,issue_description
,cause_description
,resolution_description
