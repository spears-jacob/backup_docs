CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_voice_lines
(
    `account_key`                           string,
    `encrypted_legacy_account_number_256`   string,
    `encrypted_phone_number`                string,
    `status_code`                           string,
    `status_description`                    string,
    `encrypted_account_key_256`             string,
    `encrypted_normalized_phone_number_256` string,
    `padded_encrypted_account_number_256`   string,
    `encrypted_padded_account_number_256`   string
)
    PARTITIONED BY (
        `partition_date_denver` string,
        `extract_source` string
        )
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
