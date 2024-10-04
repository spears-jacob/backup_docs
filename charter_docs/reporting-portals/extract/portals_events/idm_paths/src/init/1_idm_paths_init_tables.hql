USE ${env:TMP_db};

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable} (run_date string);
INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');

USE ${env:ENVIRONMENT};

SELECT "\n\nFor asp_idm_paths_flow\n\n";

create table IF NOT EXISTS asp_idm_paths_flow (
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
PARTITIONED BY (date_denver string);

SELECT "\n\nFor asp_idm_paths_time\n\n";

create table IF NOT EXISTS asp_idm_paths_time (
  platform string,
  referrer_link string,
  browser_name string,
  device_type string,
  pagename string,
  page_viewed_time_sec double
)
PARTITIONED BY (date_denver string);

SELECT "\n\nFor asp_idm_paths_metrics\n\n";

create VIEW IF NOT EXISTS asp_v_idm_paths_time AS
SELECT *
    FROM asp_idm_paths_time;
