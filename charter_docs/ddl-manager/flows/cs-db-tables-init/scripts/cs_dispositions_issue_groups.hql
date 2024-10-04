CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_dispositions_issue_groups
(
    issue_description string,
    id_form           string,
    issue_group       string,
    version           string
)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
