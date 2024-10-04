USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS sbnet_combined_pageviews_links_agg
(
  company String,
  account_id  String,
  visitor_id  String,
  visit_id  String,
  visit_start_timestamp String,
  previous_visit_id String,
  category  String,
  message_timestamp String,
  page_url  String,
  auth  String,
  site_name String,
  page_name String,
  site_section  String,
  sub_section String,
  link_name ARRAY<STRING>,
  link_details ARRAY<STRING>,
  referrer_link String,
  previous_page_url String,
  previous_page_name  String,
  previous_site_section String,
  previous_sub_section  String,
  search_terms  String,
  search_results ARRAY<STRING>
)
PARTITIONED BY (partition_date_denver STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

CREATE TABLE IF NOT EXISTS sbnet_combined_visits_agg
(
  company String,
  account_id  String,
  visitor_id  String,
  visit_id  String,
  visit_start_timestamp String,
  visit_duration Int,  
  previous_visit_id String,
  metric  String,
  value Int,
  country String,
  timezone  String,
  state String,
  city  String,
  zip String,
  browser String,
  os  String,
  resolution  String,
  connection_type String,
  isp String,
  ip  String
)
PARTITIONED BY (partition_date_denver STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS ORC TBLPROPERTIES('serialization.null.format'='');

-- Source file at HDFS: /archive/sbnet_chtr_events/lkp_sbnet_daily_agg_metrics.tsv
CREATE TABLE IF NOT EXISTS ${env:LKP_db}.sbnet_daily_agg_metrics(
  source string,
  metric string
 )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;