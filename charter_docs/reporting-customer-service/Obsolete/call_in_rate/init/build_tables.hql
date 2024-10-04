

CREATE TABLE IF NOT EXISTS ${env:TMP_db}.cs_care_events
(
  account_number string,
  visit_id string,
  visit_customer_type string,
  message__category string,
  message__name string,
  received__timestamp bigint,
  visit_type string
)
partitioned by
(
  partition_date_utc string
)
;


CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_calls_with_prior_visit
(
  account_number string,
  call_inbound_key bigint,
  customer_type string,
  customer_subtype string,
  agent_mso string,
  product string,
  issue_description string,
  cause_description string,
  resolution_description string,
  issue_category string,
  cause_category string,
  resolution_category string,
  resolution_type string,

  call_start_div double,
  visit_type string,
  visitstart bigint
)
PARTITIONED BY
(
  call_end_date_utc string
)
;


CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_visit_rate_4calls
(
  agent_mso string
  ,visit_type string
  ,customer_type string
  ,calls_with_visit int
  ,handled_acct_calls int
  ,total_acct_calls int
  ,total_calls int
  ,total_acct_visits int
  ,total_visits int
)
PARTITIONED BY
(
  call_date string
)
;


CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_calls_with_visits
(
  account_number string
  ,customer_type string
  ,customer_subtype string
  ,call_inbound_key string
  ,product String
  ,agent_mso String
  ,visit_type String
  ,issue_description String
  ,issue_category String
  ,cause_description String
  ,cause_category String
  ,resolution_description String
  ,resolution_category String
  ,resolution_type String
  ,minutes_to_call double
)
PARTITIONED BY
(
  call_date string
)
;
