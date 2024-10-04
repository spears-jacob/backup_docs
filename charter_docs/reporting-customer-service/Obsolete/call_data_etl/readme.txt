We can't stop calls from being duplicated when an account has both residential
and commercial services.  Therefore in the reporting, only one customer type
can be selected at a time, so that this doubling of accounts doesn't throw off
our total counts.

despite the name, the column agent_mso in cs_call_in_rate is the result of a
GROUP BY on account_agent_mso in cs_calls_with_prior_visits

Proposed new names for cs_call_in_rate:
- handled_acct_calls -> validated_calls
- total_acct_calls -> distinct_call_accts
- total_calls -> handled_calls
- total_acct_visits -> distinct_visit_accts
- total_visits -> authenticated_visits
- agent_mso -> acct_agent_mso
Then for a fiscal-month call-in rate, our definitions would be:
CIR = sum(calls_with_visit) / sum(authenticated_visits)
and
DFCR = sum(calls_with_visits)/sum(handled_calls)
