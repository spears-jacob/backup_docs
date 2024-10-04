CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_stream_agg_qa
(
    table_name          STRING,
    stream_cnts         BIGINT,
    stream_program_cnts BIGINT
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");

