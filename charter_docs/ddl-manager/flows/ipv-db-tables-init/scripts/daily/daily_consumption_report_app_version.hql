CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.daily_consumption_report_app_version
(
    total_number     bigint,
    num_streams      bigint,
    network_status   string,
    application_type string,
    playback_type    string,
    active_filt      BOOLEAN,
    hours_consumed   double,
    data_level       string,
    app_version      string
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");

