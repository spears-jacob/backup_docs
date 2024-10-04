CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_customer_params_ingest (
    company_id INT,
    rep_id INT,
    session_id STRING,
    customer_id STRING,
    company_name STRING,
    company_subdivision STRING,
    params STRING,
    param_value STRING,
    event_ts STRING,
    auth_state BOOLEAN,
    issue_id BIGINT,
    instance_ts STRING,
    param_key STRING,
    company_segments STRING,
    platform STRING,
    event_id STRING,
    referring_page_url STRING,
    partition_date STRING,
    partition_hour STRING,
    instance_hour string
)
PARTITIONED BY (instance_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
