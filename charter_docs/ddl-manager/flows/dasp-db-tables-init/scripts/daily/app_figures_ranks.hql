CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.app_figures_ranks (
 product_id string,
 country string,
 category_store string,
 category_store_id int,
 category_device string,
 category_device_id int,
 category_id string,
 category_name string,
 category_subtype string,
 position bigint,
 delta int
)
PARTITIONED BY (partition_date_denver string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;