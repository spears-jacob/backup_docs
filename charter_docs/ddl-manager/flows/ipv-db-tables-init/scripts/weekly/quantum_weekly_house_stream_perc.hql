CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_weekly_house_stream_perc
(
    grouping_id       int,
    playback_category string,
    cust_type         string,
    unique_hh         bigint,
    stream_subtype    STRING
)
    PARTITIONED BY (week_end STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
