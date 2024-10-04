USE ${env:LKP_db};

SELECt "\n\nNow Creating Look Up Tables if needed.\n\n";

CREATE TABLE IF NOT EXISTS ${env:dbprefix_event}
(
  event_id int,
  detail string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS ${env:dbprefix_browser}
(
  browser_id string,
  detail string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS ${env:dbprefix_country}
(
  country_id int,
  detail string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS ${env:dbprefix_connection_type}
(
  connection_type_id int,
  detail string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS ${env:dbprefix_javascript}
(
  javascript_id int,
  detail string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS ${env:dbprefix_language}
(
  language_id int,
  detail string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS ${env:dbprefix_os}
(
  os_id int,
  detail string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS ${env:dbprefix_resolution}
(
  resolution_id int,
  detail string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS ${env:dbprefix_search_engine}
(
  search_engine_id int,
  detail string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;
