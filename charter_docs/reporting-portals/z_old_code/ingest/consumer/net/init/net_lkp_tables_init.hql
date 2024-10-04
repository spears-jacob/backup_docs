use ${env:LKP_db};

CREATE TABLE IF NOT EXISTS net_browser
(
  browser_id String,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_connection_type
(
  connection_type_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_event
(
  event_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_country
(
  country_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_javascript
(
  javascript_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_language
(
  language_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_os
(
  os_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_resolution
(
  resolution_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_search_engine
(
  search_engine_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;
