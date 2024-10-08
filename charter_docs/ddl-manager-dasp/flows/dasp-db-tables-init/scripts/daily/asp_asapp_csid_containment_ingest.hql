CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_csid_containment_ingest (
    company_id INT,
    was_enqueued BOOLEAN,
    first_auth_external_user_id STRING,
    csid STRING,
    customer_id STRING,
    first_auth_external_user_type STRING,
    company_name STRING,
    first_auth_external_token_id STRING,
    last_auth_external_user_type STRING,
    agents_involved STRING,
    has_customer_utterance BOOLEAN,
    external_customer_id STRING,
    last_auth_source STRING,
    company_subdivision STRING,
    fgsrs_event_count SMALLINT,
    attempted_escalate BOOLEAN,
    rep_msgs INT,
    last_auth_external_user_id STRING,
    last_device_type STRING,
    is_contained BOOLEAN,
    csid_start_ts STRING,
    instance_ts STRING,
    included_issues STRING,
    last_auth_external_token_id STRING,
    first_auth_source STRING,
    messages_sent INT,
    event_count SMALLINT,
    distinct_auth_source_path STRING,
    company_segments STRING,
    last_platform STRING,
    csid_end_ts STRING,
    reps_involved STRING,
    partition_date STRING,
    partition_hour STRING,
    instance_hour string
)
PARTITIONED BY (instance_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
