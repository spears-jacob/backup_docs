-- TODO stored in daily because  only d-views use this table
CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_monthly_set_agg_page(
    mso STRING,
    application_type STRING,
    device_type STRING,
    connection_type STRING,
    network_status STRING,
    playback_type STRING,
    cust_type STRING,
    application_group_type STRING,
    app_version STRING,
    grouping_id int,
    metric_name STRING,
    metric_value DOUBLE)
    partitioned by (month_start string)
    stored as orc
    LOCATION '${s3_location}'
    TBLPROPERTIES("orc.compress"="SNAPPY");
