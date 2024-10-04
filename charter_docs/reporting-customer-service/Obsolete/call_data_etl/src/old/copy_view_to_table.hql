INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.steve_visit_rate_4calls PARTITION (call_date)
SELECT UPPER(agent_mso), UPPER(visit_type), UPPER(customer_type)
  , calls_with_visit, handled_acct_calls, total_acct_calls
  , total_calls, total_acct_visits, total_visits, call_date
FROM prod.steve_v_visit_rate_4calls
WHERE call_date <= "2019-01-02";


INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.steve_calls_with_visits PARTITION (call_date)
SELECT account_number
  ,UPPER(customer_type)
  ,UPPER(customer_subtype)
  ,call_inbound_key
  ,UPPER(product)
  ,UPPER(agent_mso)
  ,UPPER(visit_type)
  ,issue_description
  ,issue_category
  ,cause_description
  ,cause_category
  ,resolution_description
  ,resolution_category
  ,resolution_type
  ,minutes_to_call
  ,call_date
FROM prod.steve_v_calls_with_visits
WHERE call_date <= "2019-01-02";
