CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_dispositions_resolution_groups
(
    resolution_description string,
    rd_form                string,
    resolution_group       string,
    version                string
)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
