CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_customer_feedback_ingest (
    question STRING,
    last_agent_id INT,
    company_name STRING,
    question_category STRING,
    issue_id BIGINT,
    instance_ts STRING,
    question_type STRING,
    ordering INT,
    answer STRING,
    conversation_id BIGINT,
    last_rep_id INT,
    partition_date STRING,
    partition_hour STRING,
    instance_hour string
)
PARTITIONED BY (instance_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
