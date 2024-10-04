CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.mvno_accounts (
    charter_ban_aes256 string
    , mobile_account_number_aes256 string
    , phone_line_number_aes256 string
    , line_activation_date string
    , preacquisition_company string
    , activation_type string
    , line_status string
)
PARTITIONED BY (partition_date_et string)
STORED AS ORC
LOCATION 's3://pi-qtm-dasp-prod-aggregates-pii/data/incoming/mvno/mvno_accounts'
TBLPROPERTIES (
    'ORC.COMPRESS'='SNAPPY',
    'ORC.CREATE.INDEX'='true'
)
;
