CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_stream_mos_tableau
(
    stream_id                  STRING,
    mso                        STRING,
    streaming_format           STRING,
    playback_type              STRING,
    application_type           STRING,
    connection_type            STRING,
    location_zipcode           STRING,
    location_timezone          STRING,
    start_time_tz              STRING,
    end_time_tz                STRING,
    start_timestamp_no_convert STRING,
    end_timestamp_no_convert   STRING,
    mos_score                  DOUBLE
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");

