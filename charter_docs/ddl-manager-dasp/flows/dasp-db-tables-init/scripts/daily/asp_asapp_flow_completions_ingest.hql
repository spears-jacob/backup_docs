CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_flow_completions_ingest (
    customer_session_id STRING,
    company_id INT,
    customer_id STRING,
    company_name STRING,
    is_flow_success_issue BOOLEAN,
    company_subdivision STRING,
    is_flow_success_event BOOLEAN,
    success_event_ts STRING,
    external_user_id STRING,
    issue_id STRING,
    negation_event_ts STRING,
    success_rule_id STRING,
    success_event_details STRING,
    company_segments STRING,
    platform STRING,
    negation_rule_id STRING,
    conversation_id STRING,
    partition_date STRING,
    partition_hour STRING,
    success_event_hour string
)
PARTITIONED BY (success_event_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
