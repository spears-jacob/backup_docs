CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.quantum_concurrent_streams_bulkmdu_specu
(
 mso                           string
,application_type              string
,cust_type                     string
,concurrent_timestamp          string
,time_only                     string
,week_end                      string
,day_part                      string
,concurrent_streams            int
,stream_subtype                string
)
    PARTITIONED BY (denver_date STRING)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
