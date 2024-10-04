USE ${env:LKP_db};

CREATE TABLE IF NOT EXISTS sbnet_twc_global_event
(
  event_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_twc_global_browser
(
  browser_id string,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_twc_global_country
(
  country_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_twc_global_connection_type
(
  connection_type_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_twc_global_javascript
(
  javascript_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_twc_global_language
(
  language_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_twc_global_os
(
  os_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_twc_global_resolution
(
  resolution_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_twc_global_search_engine
(
  search_engine_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;
