CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_selectaction_aggregate
(
    application_name  string,
    current_page      string,
    element_name      string,
    visit_device_uuid string,
    unique_visit_id   string
)
    PARTITIONED BY
        (
        partition_date_utc string
        )
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
;
