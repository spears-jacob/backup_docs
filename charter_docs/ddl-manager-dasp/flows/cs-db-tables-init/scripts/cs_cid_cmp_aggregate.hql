CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_cid_cmp_aggregate
(
    application_name       STRING,
    message_name           STRING,
    count_of_events        INT,
    count_of_unique_users  INT,
    count_of_users         INT,
    count_of_visits        INT,
    campaign_id            STRING,
    page_title             STRING,
    page_id                STRING,
    page_name              STRING
)
PARTITIONED BY (grain STRING, label_date_denver STRING)
STORED AS PARQUET
LOCATION '${s3_location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
;
