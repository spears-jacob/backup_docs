CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_rep_attributes_ingest (
    rep_id INT,
    company_name STRING,
    created_ts STRING,
    rep_attribute_id BIGINT,
    external_rep_id STRING,
    external_agent_id STRING,
    attribute_value STRING,
    attribute_name STRING,
    agent_attribute_id BIGINT,
    agent_id INT,
    partition_hour STRING
)
PARTITIONED BY (partition_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
