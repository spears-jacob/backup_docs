CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_bounces_entries
(
  page_name STRING,
  entries INT,
  bounces INT
)
PARTITIONED BY (domain STRING, date_denver STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY"); 
