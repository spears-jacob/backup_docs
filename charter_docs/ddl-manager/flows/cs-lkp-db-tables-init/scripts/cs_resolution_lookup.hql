CREATE EXTERNAL TABLE if not exists ${db_name}.cs_resolution_lookup
(
    `resolution_description` string,
    `resolution_category`    string,
    `resolution_type`        string
)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
