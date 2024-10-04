CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_issue_queues_ingest (
    rep_id INT,
    enter_queue_flow_name STRING,
    abandoned BOOLEAN,
    company_name STRING,
    enter_queue_eventflags INT,
    enqueue_time DOUBLE,
    company_subdivision STRING,
    queue_id STRING,
    issue_id BIGINT,
    enter_queue_eventtype STRING,
    instance_ts STRING,
    enter_queue_message_name STRING,
    exit_queue_eventflags INT,
    enter_queue_ts STRING,
    exit_queue_ts STRING,
    company_segments STRING,
    conversation_id BIGINT,
    exit_queue_eventtype STRING,
    queue_name STRING,
    agent_id INT,
    partition_date STRING,
    partition_hour STRING,
    instance_hour string
)
PARTITIONED BY (instance_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
