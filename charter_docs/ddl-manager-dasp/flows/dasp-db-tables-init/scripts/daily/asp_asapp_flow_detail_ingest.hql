CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_flow_detail_ingest (
    link_resolved_pdl STRING,
    link_resolved_pil STRING,
    session_id STRING,
    company_name STRING,
    event_name STRING,
    event_ts STRING,
    issue_id STRING,
    flow_name STRING,
    event_type STRING,
    flow_id STRING,
    conversation_id STRING,
    partition_date STRING,
    partition_hour STRING,
    event_hour string
)
PARTITIONED BY (event_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
