CREATE EXTERNAL TABLE if not exists ${db_name}.quantum_customer_engagement_report
(
    grouping_id      bigint,
    acct_num         string,
    company          string,
    application_type string,
    playback_type    string,
    billing_division string,
    device_model     string,
    account_zip_code string,
    total_visits     bigint,
    total_streams    bigint,
    total_sec        double,
    last_date_used   STRING
)
    PARTITIONED BY (week_end STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
