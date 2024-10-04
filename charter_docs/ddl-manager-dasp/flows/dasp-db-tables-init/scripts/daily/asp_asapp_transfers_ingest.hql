CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_transfers_ingest (
    company_id INT,
    rep_id INT,
    actual_rep_transfer INT,
    requested_rep_transfer STRING,
    company_name STRING,
    actual_agent_transfer INT,
    group_transfer_from_name STRING,
    is_auto_transfer BOOLEAN,
    company_subdivision STRING,
    transfer_button_clicks INT,
    requested_agent_transfer STRING,
    accepted BOOLEAN,
    timestamp_req STRING,
    group_transfer_from STRING,
    issue_id BIGINT,
    group_transfer_to STRING,
    exit_transfer_event_type STRING,
    instance_ts STRING,
    timestamp_reply STRING,
    company_segments STRING,
    conversation_id BIGINT,
    group_transfer_to_name STRING,
    agent_id INT,
    partition_date STRING,
    partition_hour STRING,
    instance_hour string
)
PARTITIONED BY (instance_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
