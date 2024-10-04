CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_rep_utilized_ingest (
    company_id INT,
    rep_id INT,
    act_ratio DOUBLE,
    lin_ute_avail_min FLOAT,
    company_name STRING,
    cum_ute_avail_min FLOAT,
    lin_ute_busy_min FLOAT,
    company_subdivision STRING,
    ute_ratio DOUBLE,
    lin_avail_min FLOAT,
    cum_ute_prebreak_min FLOAT,
    rep_name STRING,
    lin_prebreak_min FLOAT,
    lin_logged_in_min FLOAT,
    cum_ute_busy_min FLOAT,
    instance_ts STRING,
    busy_clicks_ct INT,
    lin_ute_prebreak_min FLOAT,
    lin_busy_min FLOAT,
    cum_logged_in_min FLOAT,
    lin_ute_total_min FLOAT,
    max_slots INT,
    company_segments STRING,
    cum_prebreak_min FLOAT,
    labor_min FLOAT,
    cum_avail_min FLOAT,
    cum_ute_total_min FLOAT,
    cum_busy_min FLOAT,
    partition_date STRING,
    partition_hour STRING,
    instance_hour string
)
PARTITIONED BY (instance_date string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
