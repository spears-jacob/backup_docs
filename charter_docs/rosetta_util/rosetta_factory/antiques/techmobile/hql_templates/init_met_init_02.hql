

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
message_timestamp,
receivedDate
)
PARTITIONED BY (partition_date_utc STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - With PII (3 Years)')
;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
