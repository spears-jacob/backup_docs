CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_support_quantum_events (
  application_name                 string, 
  visit_id                         string, 
  device_id                        string, 
  page_name                        string, 
  journey                          string, 
  sub_journey                      string, 
  visit_unique_id                  string, 
  account_number                   string, 
  received__timestamp              bigint, 
  message__name                    string, 
  operation__operation_type        string, 
  search_id                        string, 
  search_text                      string, 
  seq_num                          int, 
  element_url                      string, 
  helpful_yes                      int, 
  helpful_no                       int, 
  wasnt_what_i_searched            int, 
  incorrect_info                   int, 
  confusing                        int, 
  page_id                          string
)
PARTITIONED BY (
  denver_date STRING
)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY") --justincase
;
