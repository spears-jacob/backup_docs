CREATE EXTERNAL TABLE if not exists ${db_name}.quantum_customer_engagement_report_daily
(
    grouping_id      int,
    acct_num         string,
    company          string,
    application_type string,
    playback_type    string,
    billing_division string,
    device_model     string,
    account_zip_code string,
    total_visits     int,
    total_streams    int,
    total_sec        double,
    last_date_used   string
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
