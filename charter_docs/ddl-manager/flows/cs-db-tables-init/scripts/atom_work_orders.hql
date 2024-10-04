CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_work_orders
(
    `legacy_company`                                 string,
    `account_key`                                    string,
    `order_number`                                   string,
    `job_number`                                     string,
    `job_reason_code`                                string,
    `job_reason_code_description`                    string,
    `job_resolution_code`                            string,
    `job_resolution_code_description`                string,
    `job_type_code`                                  string,
    `job_type_code_description`                      string,
    `job_category_code`                              string,
    `job_class_code`                                 string,
    `job_class_category_code`                        string,
    `installation_category`                          string,
    `includes_truck_roll`                            boolean,
    `truck_roll_reason_code_category`                string,
    `technician_id`                                  string,
    `job_status_code`                                string,
    `encrypted_technician_contracting_firm_name_256` string,
    `encrypted_technician_first_name_256`            string,
    `encrypted_technician_last_name_256`             string,
    `job_entry_date_eastern`                         string,
    `job_completed_date_eastern`                     string,
    `service_code`                                   string,
    `service_code_description`                       string,
    `spectrum_guide_install_type`                    string,
    `encrypted_account_key_256`                      string,
    `encrypted_technician_id_256`                    string
)
    PARTITIONED BY (
        `partition_date_denver` string,
        `extract_source` string
        )
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
