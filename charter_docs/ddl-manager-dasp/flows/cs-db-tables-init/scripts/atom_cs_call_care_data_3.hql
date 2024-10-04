-- todo to sure about partitition columns
CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.atom_cs_call_care_data_3
(
    `call_inbound_key`                                   string,
    `call_id`                                            string,
    `call_start_date_utc`                                string,
    `call_start_time_utc`                                string,
    `call_end_time_utc`                                  string,
    `call_start_datetime_utc`                            string,
    `call_end_datetime_utc`                              string,
    `call_start_timestamp_utc`                           bigint,
    `call_end_timestamp_utc`                             bigint,
    `previous_call_time_utc`                             bigint,
    `segment_id`                                         string,
    `segment_number`                                     string,
    `segment_status_disposition`                         string,
    `segment_start_time_utc`                             string,
    `segment_end_time_utc`                               string,
    `segment_start_datetime_utc`                         string,
    `segment_end_datetime_utc`                           string,
    `segment_start_timestamp_utc`                        bigint,
    `segment_end_timestamp_utc`                          bigint,
    `segment_duration_seconds`                           bigint,
    `segment_duration_minutes`                           double,
    `segment_handled_flag`                               int,
    `encrypted_account_number_256`                       string,
    `encrypted_account_key_256`                          string,
    `encrypted_customer_account_number_256`              string,
    `customer_type`                                      string,
    `customer_subtype`                                   string,
    `truck_roll_flag`                                    boolean,
    `encrypted_notes_txt_256`                            string,
    `resolution_description`                             string,
    `cause_description`                                  string,
    `issue_description`                                  string,
    `company_code`                                       string,
    `service_call_tracker_id`                            string,
    `created_on`                                         string,
    `encrypted_phone_number_from_tracker_256`            string,
    `encrypted_normalized_phone_number_from_tracker_256` string,
    `call_type`                                          string,
    `split_sum_desc`                                     string,
    `location_name`                                      string,
    `care_center_management_name`                        string,
    `agent_job_role_name`                                string,
    `agent_effective_hire_date`                          string,
    `agent_mso`                                          string,
    `eduid`                                              string,
    `last_handled_segment_flag`                          boolean,
    `record_update_timestamp`                            string,
    `source`                                             string,
    `enhanced_account_number`                            boolean,
    `account_agent_mso`                                  string,
    `customer_call_count_indicator`                      string,
    `call_owner`                                         string,
    `product`                                            string,
    `enhanced_mso`                                       boolean,
    `call_handled_flag`                                  int,
    `encrypted_padded_account_number_256`                string,
    `encrypted_created_by_256`                           string,
    `call_end_date_east`                                 string,
    `call_end_date_utc`                                  string
)
    STORED AS ORC
    LOCATION '${s3_location}'
    TBLPROPERTIES ("orc.compress" = "SNAPPY")
