CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_specmobile_login
(
    login_timestamp bigint,
    account_number string,
    division_id string,
    division string,
    site_sys string,
    prn string,
    agn string
)
    PARTITIONED BY (date_denver string)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY");
