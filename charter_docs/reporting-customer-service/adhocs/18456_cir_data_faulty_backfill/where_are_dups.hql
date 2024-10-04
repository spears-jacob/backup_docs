SELECT
call_date
,agent_mso
,visit_type
,count(total_calls) as count
FROM prod.cs_call_in_rate_corrected_20191204
WHERE call_date>='2019-11-01'
GROUP BY call_date, agent_mso,visit_type
HAVING count>1
