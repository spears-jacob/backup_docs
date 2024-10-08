SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.cli.print.header=true;

--query for researching why we're failing on as_ns_check_prior_visit_segment_counts.
--Once you've found a call_inbound_key that doesn't match (that will have been generated by the check script)
--plug that key in here.  This will pull the data from both tables, so you can figure out what's causing the problem

SELECT
account_key
, customer_type
,call_inbound_key
, agent_mso
--, account_agent_mso
, issue_description
, resolution_description
, cause_description
, segment_handled_flag
, segment_id

FROM
test.cs_call_care_data_amo_julyv3
--prod.cs_call_care_data
WHERE
--segment_id in ('2061524210509-36334353-1')
call_inbound_key in ('2061579262870')
;

SELECT
account_key
, segment_id
, customer_type
,call_inbound_key
, agent_mso
--, account_agent_mso
, issue_description
, resolution_description
, cause_description
FROM
test.cs_calls_with_prior_visits_amo_julyv3
--test.cs_call_care_data_amo
--prod.cs_call_care_data
WHERE
--segment_id in ('2061527959131-39228090-2')
call_inbound_key in ('2061579262870')
;
