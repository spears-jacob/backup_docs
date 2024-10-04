CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_dispositions
(
    issue_description      string,
    id_form                string,
    cause_description      string,
    cd_form                string,
    resolution_description string,
    rd_form                string
)
    PARTITIONED BY (partition_month date)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
