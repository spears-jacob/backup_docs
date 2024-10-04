CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_rep_augmentation_ingest (
    is_rep_resolved BOOLEAN,
    company_id INT,
    rep_id INT,
    customer_id STRING,
    company_name STRING,
    auto_complete_msgs INT,
    auto_suggest_msgs INT,
    external_customer_id STRING,
    company_subdivision STRING,
    custom_auto_complete_msgs INT,
    conversation_end_ts STRING,
    issue_id BIGINT,
    instance_ts STRING,
    custom_auto_suggest_msgs INT,
    kb_recommendation_msgs INT,
    kb_search_msgs INT,
    is_billable BOOLEAN,
    company_segments STRING,
    did_customer_timeout BOOLEAN,
    conversation_id BIGINT,
    drawer_msgs INT,
    agent_id INT,
    partition_date STRING,
    partition_hour STRING,
    instance_hour string
)
PARTITIONED BY (instance_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
