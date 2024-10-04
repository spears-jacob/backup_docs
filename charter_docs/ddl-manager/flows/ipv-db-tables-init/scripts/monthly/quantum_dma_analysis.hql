CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_dma_analysis
(
    grouping_id       STRING,
    mso               STRING,
    application_type  STRING,
    location          STRING,
    dma               STRING,
    dma_name          STRING,
    unique_households BIGINT,
    unique_visits     BIGINT,
    location_is_encrypted BOOLEAN,
    report_group      STRING
)
    PARTITIONED BY (month_start STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
