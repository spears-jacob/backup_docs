SELECT * FROM 
prod.cs_call_in_rate a
JOIN prod.cs_call_in_rate b
ON a.call_date=b.call_date 
 AND a.visit_type=b.visit_type
 AND a.agent_mso=b.agent_mso
WHERE
  a.call_date>='2019-11-01'
  AND b.call_date>='2019-11-01'
  AND	 
	(
	a.calls_with_visit <>b.calls_with_visit
	OR a.handled_acct_calls<>b.handled_acct_calls
	OR a.total_acct_calls<>b.total_acct_calls
	OR a.total_calls <>b.total_calls
	OR a.total_acct_visits<>b.total_acct_visits
	OR a.total_visits<>b.total_visits
	)
