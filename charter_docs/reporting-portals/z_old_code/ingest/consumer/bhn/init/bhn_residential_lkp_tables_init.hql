USE ${env:LKP_db};

CREATE TABLE IF NOT EXISTS bhn_residential_event
(
  event_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS bhn_residential_browser
(
  browser_id string,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS bhn_residential_country
(
  country_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS bhn_residential_connection_type
(
  connection_type_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS bhn_residential_javascript
(
  javascript_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS bhn_residential_language
(
  language_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS bhn_residential_os
(
  os_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS bhn_residential_resolution
(
  resolution_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS bhn_residential_search_engine
(
  search_engine_id int,
  detail string
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;
