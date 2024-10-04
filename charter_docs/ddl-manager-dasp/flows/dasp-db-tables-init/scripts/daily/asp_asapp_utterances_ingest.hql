CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_utterances_ingest (
    sent_to_rep BOOLEAN,
    sequence_id STRING,
    company_name STRING,
    sender_id STRING,
    company_subdivision STRING,
    sender_type STRING,
    created_ts STRING,
    issue_id BIGINT,
    instance_ts STRING,
    sent_to_agent BOOLEAN,
    utterance STRING,
    company_segments STRING,
    conversation_id BIGINT,
    utterance_type STRING,
    partition_date STRING,
    partition_hour STRING,
    instance_hour string
)
PARTITIONED BY (instance_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
