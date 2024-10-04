CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_mos_tableau_weekly
(
    avg_stream_mos_perc double,
    playback_type       string,
    company_mos         string,
    application_type    string,
    grouping_id         int
)
    PARTITIONED BY (week_end STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
