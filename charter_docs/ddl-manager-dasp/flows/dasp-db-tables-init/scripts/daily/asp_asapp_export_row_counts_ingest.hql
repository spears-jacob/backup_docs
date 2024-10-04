CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_export_row_counts_ingest (
    export_name STRING,
    company_name STRING,
    export_interval STRING,
    exported_rows INT,
    export_date STRING,
    partition_date STRING,
    partition_hour STRING
)
PARTITIONED BY (partition_export_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
