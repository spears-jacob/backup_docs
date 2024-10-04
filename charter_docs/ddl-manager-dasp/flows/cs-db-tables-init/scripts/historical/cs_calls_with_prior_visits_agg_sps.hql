CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_calls_with_prior_visits_agg_sps
(
    customer_type          string comment "Indicates whether caller was identified as residential, small business, etc.",
    agent_mso              string comment "The MSO of the agent who took the call",
    account_agent_mso      string comment "When available, the mso of the account. If that's not available, agent_mso",
    visit_type             string comment "Which Portal was visited",
    issue_description      string comment "Lowest level of disposition granularity",
    cause_description      string comment "Medium level of disposition granularity",
    resolution_description string comment "Highest level of dispositon granularity",
    segments               bigint comment "COUNT of segments where segment_handled_flag = 1 and account_number is not enhanced",
    calls                  bigint comment "COUNT of distinct calls where segment_handled_flag = 1 and account_number is not enhanced",
    accounts               bigint comment "COUNT of distinct accounts where segment_handled_flag = 1 and account_number is not enhanced",
    sps_flag               string
)
    PARTITIONED BY (call_date string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY") 
