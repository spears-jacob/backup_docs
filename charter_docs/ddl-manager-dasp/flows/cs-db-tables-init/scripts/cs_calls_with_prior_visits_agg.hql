CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_calls_with_prior_visits_agg
(
    customer_type                         string comment "Indicates whether caller was identified as residential, small business, etc.",
    agent_mso                             string comment "The MSO of the agent who took the call",
    account_agent_mso                     string comment "When available, the mso of the account. If that's not available, agent_mso",
    visit_type                            string comment "Which Portal was visited",
    primary_auto_disposition_issue        string comment "Lowest level of disposition granularity created by automated voice analysis",
    primary_auto_disposition_cause        string  comment "Medium level of disposition granularity created by automated voice analysis",
    primary_manual_disposition_issue      string  comment "Lowest level of disposition granularity entered manually by agent",
    primary_manual_disposition_cause      string  comment "Medium level of disposition granularity entered manually by agent",
    primary_manual_disposition_resolution string  comment "Highest level of disposition granularity entered manually by agent",
    issue_description                     string comment "Lowest level of disposition granularity",
    cause_description                     string comment "Medium level of disposition granularity",
    resolution_description                string comment "Highest level of dispositon granularity",
    segments                              bigint comment "COUNT of segments where segment_handled_flag = 1 and account_number is not enhanced",
    calls                                 bigint comment "COUNT of distinct calls where segment_handled_flag = 1 and account_number is not enhanced",
    accounts                              bigint comment "COUNT of distinct accounts where segment_handled_flag = 1 and account_number is not enhanced"
)
    PARTITIONED BY (call_date string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
