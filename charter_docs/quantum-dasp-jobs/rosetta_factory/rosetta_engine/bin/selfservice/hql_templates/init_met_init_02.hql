--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
portals_unique_acct_key STRING,
activated_experiments map<string,string>,
technology_type STRING,
calls_within_24_hrs INT
),

PARTITIONED BY (denver_date STRING)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ('orc.compress'='SNAPPY',
               'retention_policy'='Aggregate - With PII (3 Years)');
