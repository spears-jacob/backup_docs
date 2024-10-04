CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_quantum_cid_pageviews
(
    application_name     string,
    application_api_host string,
    URL_new              string,
    CID                  string,
    CMP                  string,
    message_name         string,
    visit_device_uuid    string,
    unique_visit_id      string
)
    PARTITIONED BY
        (
        partition_date_utc date
        )
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;

