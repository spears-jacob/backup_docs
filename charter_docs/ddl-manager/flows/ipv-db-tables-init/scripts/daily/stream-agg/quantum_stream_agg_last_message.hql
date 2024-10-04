CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_stream_agg_last_message
(
    stream_id                    STRING,
    stream_program_id            STRING,
    grouping_id                  INT,
    last_message_sequence_number INT,
    last_message                 STRING,
    last_five_message            ARRAY<STRING>
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");


