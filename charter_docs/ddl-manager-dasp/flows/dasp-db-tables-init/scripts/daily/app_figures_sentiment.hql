CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.app_figures_sentiment (
 author string,
 title string,
 review string,
 original_title string,
 original_review string,
 stars double,
 iso string,
 version string,
 product_name string,
 product_id string,
 vendor_id string,
 store string,
 weight int,
 id string,
 language string,
 sentiment double,
 review_timestamp string
)
PARTITIONED BY (partition_date_denver string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY")
;
