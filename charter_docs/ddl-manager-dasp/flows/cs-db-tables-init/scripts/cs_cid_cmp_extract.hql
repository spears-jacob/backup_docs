CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_cid_cmp_extract
(
    application_name    string,
    message_name        string,
    visit_device_uuid   string,
    unique_visit_id     string,
    campaign_id         string,
    page_title          string,
    page_id             string,
    page_name           string
)
PARTITIONED BY (denver_date string)
STORED AS PARQUET
LOCATION '${s3_location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
;
