CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_reps_ingest (
    rep_id INT,
    name STRING,
    crm_rep_id STRING,
    crm_agent_id STRING,
    company_name STRING,
    agent_status INT,
    max_slot INT,
    disabled_time STRING,
    created_ts STRING,
    rep_status INT,
    agent_id INT,
    partition_hour STRING
)
PARTITIONED BY (partition_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
