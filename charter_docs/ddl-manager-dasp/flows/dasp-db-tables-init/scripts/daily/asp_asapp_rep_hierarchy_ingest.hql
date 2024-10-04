CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_asapp_rep_hierarchy_ingest (
    company_name STRING,
    superior_rep_id INT,
    subordinate_agent_id INT,
    superior_agent_id INT,
    subordinate_rep_id INT,
    reporting_relationship STRING,
    partition_hour STRING
)
PARTITIONED BY (partition_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
