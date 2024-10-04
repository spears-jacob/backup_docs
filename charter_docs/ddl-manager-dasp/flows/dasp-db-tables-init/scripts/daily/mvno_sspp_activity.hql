CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.mvno_sspp_activity
(
    charter_ban_aes256           string,
    mobile_account_number_aes256 string,
    application_name             string,
    minvisitdate                 string
)
    PARTITIONED BY (`line_activation_date` string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
