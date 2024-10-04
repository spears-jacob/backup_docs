--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
calls_within_24_hrs INT
)

PARTITIONED BY (partition_date_utc STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ('orc.compress'='SNAPPY',
               'retention_policy'='Aggregate - With PII (3 Years)');
