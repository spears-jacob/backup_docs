CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_idm_paths_flow (
  platform string,
  referrer_link string,
  browser_name string,
  device_type string,
  visit_id string,
  last_pagename string,
  last_app_section string,
  last_api_code string,
  last_api_text string,
  first_event string,
  last_event string,
  last_msg_name string,
  msg_before_first_event string,
  msg_after_last_event string,
  last_order int,
  visit_max int
)
PARTITIONED BY (date_denver string)
STORED AS ORC
LOCATION '${s3_location}'
TBLPROPERTIES ("orc.compress"="SNAPPY");
