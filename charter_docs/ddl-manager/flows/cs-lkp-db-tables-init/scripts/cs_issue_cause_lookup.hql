CREATE EXTERNAL TABLE if not exists ${db_name}.cs_issue_cause_lookup
(
    `call_group`        string,
    `issue_description` string,
    `cause_description` string,
    `issue_category`    string,
    `cause_category`    string
)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ('orc.compress' = 'SNAPPY');
