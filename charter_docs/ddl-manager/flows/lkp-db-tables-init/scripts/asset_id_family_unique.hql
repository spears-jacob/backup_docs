CREATE EXTERNAL TABLE if not exists ${db_name}.asset_id_family_unique
(
    providerassetid   string,
    network           string,
    programmer_family string
)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
