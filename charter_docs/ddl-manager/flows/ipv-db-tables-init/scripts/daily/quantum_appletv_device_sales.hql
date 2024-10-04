CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_appletv_device_sales
(
    account_number  STRING,
    solo_account_id STRING,
    legacy_mso      STRING,
    db_root         STRING,
    level_1         STRING,
    level_2         STRING,
    level_3         STRING,
    level_4         STRING,
    status          STRING,
    serial_number   STRING,
    model           STRING,
    sale_type       STRING,
    purchase_date   STRING,
    sales_channel   STRING,
    report_date     STRING
)
    PARTITIONED BY (partition_date string, partition_hour string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
