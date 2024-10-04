CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_call_care_data_agg_no_call_type
(
    customer_type            string comment "Indicates whether caller was identified as residential, small business, etc.",
    agent_mso                string comment "The MSO of the agent who took the call",
    account_agent_mso        string comment "When available, the mso of the account. If that's not available, agent_mso",
    issue_description        string comment "Lowest level of disposition granularity",
    cause_description        string comment "Medium level of disposition granularity",
    resolution_description   string comment "Highest level of dispositon granularity",
    truck_roll_flag          boolean comment "Whether or not a service call was created from this call",
    segment_handled_flag     int comment "Whether the segment was handled by an agent or not",
    segments                 bigint comment "COUNT of segments that fit into these categories",
    calls                    bigint comment "COUNT of distinct calls that fit into these categories",
    accounts                 bigint comment "COUNT of distinct accounts that fit into these categories",
    segment_duration_minutes double comment "SUM of total talk time"
)
    PARTITIONED BY (call_end_date_utc date comment "Date when call ended")
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
