CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.daily_consumption_report_hist
(
    data_level       string,
    bin_c            bigint,
    bin_cnt          bigint,
    network_status   string,
    application_type string,
    playback_type    string,
    active_filt      boolean
) PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
