SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

set hive.cli.print.header=true; 

SELECT * 
FROM prod.cs_calls_with_prior_visits 
WHERE call_inbound_key='2061187439740' AND call_date>='2018-12-01';

SELECT 
	call_inbound_key
	,agent_mso
	,segment_id
	,issue_description
	,cause_description
	,resolution_description
	,segment_handled_flag
	,call_start_timestamp_utc
FROM prod.cs_call_care_data
WHERE call_inbound_key='2061187439740' AND call_end_date_utc>='2018-12-01';

	
--SELECT 
--	call_inbound_key
--	,segment_id
--	,issue_description
--	,cause_description
--	,resolution_description
--	,segment_handled_flag
--	,call_start_timestamp_utc
--	, agent_mso
--FROM prod.cs_call_data
--WHERE call_inbound_key='2061187439740' AND call_end_date_utc>='2018-12-01';

