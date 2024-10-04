USE ${env:LKP_db};

CREATE TABLE IF NOT EXISTS net_DEV_browser
(
  browser_id String,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_DEV_connection_type
(
  connection_type_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_DEV_event
(
  event_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_DEV_country
(
  country_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_DEV_javascript
(
  javascript_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_DEV_language
(
  language_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_DEV_os
(
  os_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_DEV_resolution
(
  resolution_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;

CREATE TABLE IF NOT EXISTS net_DEV_search_engine
(
  search_engine_id int,
  detail String
)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile;
