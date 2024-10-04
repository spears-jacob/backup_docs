CREATE EXTERNAL TABLE ${db_name}.quality_expected_unexpected_mapping (
    `code`   string,
    `status` string
)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
