CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_unknown_api_paths
(
    partition_date_utc STRING,
    apipath            STRING,
    counts             INT
)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
