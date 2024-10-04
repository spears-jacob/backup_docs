CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_dispositions_cause_groups
(
    cause_description string,
    cd_form           string,
    cause_group       string,
    version           string
)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
