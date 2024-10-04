CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_intents_ingest (
    name STRING,
    default_disambiguation BOOLEAN,
    company_name STRING,
    short_description STRING,
    code STRING,
    flow_name STRING,
    intent_type STRING,
    actions STRING,
    partition_hour STRING
)
PARTITIONED BY (partition_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
