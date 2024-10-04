SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true; 

SELECT DISTINCT
	cd.call_inbound_key,
         cd.call_end_date_utc as call_date,
         cd.customer_type,
         cd.product,
	 cd.agent_mso,
	 cv.visit_type,
         cd.issue_description,
         cd.cause_description,
         cd.resolution_description
FROM prod.cs_call_data cd
LEFT JOIN 
	(SELECT DISTINCT call_inbound_key, visit_type
	FROM prod.cs_calls_with_prior_visits) cv
 ON cd.call_inbound_key = cv.call_inbound_key
WHERE cd.segment_handled_flag = true                                            -- Identifies handled segments where the agent spoke with the customer
AND cd.call_end_date_utc>='2018-12-01'
AND cd.call_end_date_utc<'2019-01-01'
AND cd.agent_mso='TWC'
AND lower(cv.visit_type)='myspectrum'
AND issue_description='Billing Inquiries'
AND cd.cause_description='Credit/Refund Inquiry'
--AND cd.resolution_description='Added Services'
