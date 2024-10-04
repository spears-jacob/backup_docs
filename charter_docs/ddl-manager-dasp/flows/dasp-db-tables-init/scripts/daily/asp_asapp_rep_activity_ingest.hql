CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_rep_activity_ingest (
    status_description STRING,
    rep_id INT,
    company_name STRING,
    company_subdivision STRING,
    status_id STRING,
    in_status_starting_ts STRING,
    rep_name STRING,
    agent_name STRING,
    total_status_time DOUBLE,
    instance_ts STRING,
    cumul_ute_time DOUBLE,
    unutilized_time DOUBLE,
    linear_ute_time DOUBLE,
    window_status_time FLOAT,
    max_slots INT,
    company_segments STRING,
    agent_id INT,
    partition_date STRING,
    partition_hour STRING,
    instance_hour string
)
PARTITIONED BY (instance_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
