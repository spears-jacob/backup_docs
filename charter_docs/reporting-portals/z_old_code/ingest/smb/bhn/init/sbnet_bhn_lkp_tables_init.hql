USE ${env:LKP_db};

CREATE TABLE IF NOT EXISTS sbnet_bhn_event
(
  event_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_bhn_browser
(
  browser_id string,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_bhn_country
(
  country_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_bhn_connection_type
(
  connection_type_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_bhn_javascript
(
  javascript_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_bhn_language
(
  language_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_bhn_os
(
  os_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_bhn_resolution
(
  resolution_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS sbnet_bhn_search_engine
(
  search_engine_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;
