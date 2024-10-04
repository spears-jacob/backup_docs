SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;


--query for finding what data is in a table for a particular segment id

SELECT
account_key
, customer_type
,call_inbound_key
, agent_mso
--, account_agent_mso
, issue_description
, resolution_description
, cause_description
, segment_id
FROM
--test.cs_call_care_data_amo_june
test.cs_call_care_data_amo
--prod.cs_call_care_data
WHERE
segment_id in ('2061524211362-36352375-1')
--call_inbound_key in ('2061524207088')
;

--SELECT
--account_key
--, customer_type
--,call_inbound_key
--, agent_mso
--, account_agent_mso
--, issue_description
--, resolution_description
--, cause_description
--FROM
--test.cs_calls_with_prior_visits
--test.cs_call_care_data_amo
--prod.cs_call_care_data
--WHERE
--segment_id in ('2061527959131-39228090-2')
--call_inbound_key in ('2061524207088')
--;
