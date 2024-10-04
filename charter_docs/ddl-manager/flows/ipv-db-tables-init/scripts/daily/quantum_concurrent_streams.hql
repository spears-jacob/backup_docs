CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_concurrent_streams
(
    mso                  STRING,
    application_type     STRING,
    concurrent_timestamp STRING,
    time_only            STRING,
    week_end             STRING,
    day_part             STRING,
    concurrent_streams   INT
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
