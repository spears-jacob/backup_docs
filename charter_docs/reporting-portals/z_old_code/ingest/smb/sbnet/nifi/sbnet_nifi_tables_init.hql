USE ${env:LKP_db};

CREATE TABLE IF NOT EXISTS nifi_sbnet_event
(
  event_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_browser
(
  browser_id string,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_country
(
  country_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_connection_type
(
  connection_type_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_javascript
(
  javascript_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_language
(
  language_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_os
(
  os_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_resolution
(
  resolution_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_search_engine
(
  search_engine_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE EXTERNAL TABLE IF NOT EXISTS ${env:TMP_db}.nifi_sbnet_raw
(
  accept_language String,
  browser String,
  browser_height Int,
  browser_width Int,
  c_color String,
  campaign String,
  carrier String,
  channel String,
  click_action String,
  click_action_type Int,
  click_context String,
  click_context_type Int,
  click_sourceid Int,
  click_tag String,
  code_ver String,
  color Int,
  connection_type Int,
  cookies String,
  country Int,
  ct_connect_type String,
  curr_factor Int,
  curr_rate Double,
  currency String,
  cust_hit_time_gmt BigInt,
  cust_visid String,
  daily_visitor Int,
  date_time String,
  domain String,
  duplicate_events String,
  duplicate_purchase Int,
  duplicated_from String,
  ef_id String,
  evar1 String,
  evar2 String,
  evar3 String,
  evar4 String,
  evar5 String,
  evar6 String,
  evar7 String,
  evar8 String,
  evar9 String,
  evar10 String,
  evar11 String,
  evar12 String,
  evar13 String,
  evar14 String,
  evar15 String,
  evar16 String,
  evar17 String,
  evar18 String,
  evar19 String,
  evar20 String,
  evar21 String,
  evar22 String,
  evar23 String,
  evar24 String,
  evar25 String,
  evar26 String,
  evar27 String,
  evar28 String,
  evar29 String,
  evar30 String,
  evar31 String,
  evar32 String,
  evar33 String,
  evar34 String,
  evar35 String,
  evar36 String,
  evar37 String,
  evar38 String,
  evar39 String,
  evar40 String,
  evar41 String,
  evar42 String,
  evar43 String,
  evar44 String,
  evar45 String,
  evar46 String,
  evar47 String,
  evar48 String,
  evar49 String,
  evar50 String,
  evar51 String,
  evar52 String,
  evar53 String,
  evar54 String,
  evar55 String,
  evar56 String,
  evar57 String,
  evar58 String,
  evar59 String,
  evar60 String,
  evar61 String,
  evar62 String,
  evar63 String,
  evar64 String,
  evar65 String,
  evar66 String,
  evar67 String,
  evar68 String,
  evar69 String,
  evar70 String,
  evar71 String,
  evar72 String,
  evar73 String,
  evar74 String,
  evar75 String,
  evar76 String,
  evar77 String,
  evar78 String,
  evar79 String,
  evar80 String,
  evar81 String,
  evar82 String,
  evar83 String,
  evar84 String,
  evar85 String,
  evar86 String,
  evar87 String,
  evar88 String,
  evar89 String,
  evar90 String,
  evar91 String,
  evar92 String,
  evar93 String,
  evar94 String,
  evar95 String,
  evar96 String,
  evar97 String,
  evar98 String,
  evar99 String,
  evar100 String,
  event_list String,
  exclude_hit Int,
  first_hit_page_url String,
  first_hit_pagename String,
  first_hit_referrer String,
  first_hit_time_gmt BigInt,
  geo_city String,
  geo_country String,
  geo_dma Int,
  geo_region String,
  geo_zip String,
  hier1 String,
  hier2 String,
  hier3 String,
  hier4 String,
  hier5 String,
  hit_source Int,
  hit_time_gmt BigInt,
  hitid_high BigInt,
  hitid_low BigInt,
  homepage String,
  hourly_visitor Int,
  ip String,
  ip2 String,
  j_jscript String,
  java_enabled String,
  javascript Int,
  language Int,
  last_hit_time_gmt BigInt,
  last_purchase_num Int,
  last_purchase_time_gmt BigInt,
  mc_audiences String,
  mcvisid String,
  mobile_id Int,
  mobileaction String,
  mobileappid String,
  mobilecampaigncontent String,
  mobilecampaignmedium String,
  mobilecampaignname String,
  mobilecampaignsource String,
  mobilecampaignterm String,
  mobiledayofweek String,
  mobiledayssincefirstuse String,
  mobiledayssincelastuse String,
  mobiledevice String,
  mobilehourofday String,
  mobileinstalldate String,
  mobilelaunchnumber String,
  mobileltv String,
  mobilemessageid String,
  mobilemessageonline String,
  mobileosversion String,
  mobilepushoptin String,
  mobilepushpayloadid String,
  mobileresolution String,
  monthly_visitor Int,
  mvvar1 String,
  mvvar2 String,
  mvvar3 String,
  namespace String,
  new_visit Int,
  os Int,
  p_plugins String,
  page_event Int,
  page_event_var1 String,
  page_event_var2 String,
  page_event_var3 String,
  page_type String,
  page_url String,
  pagename String,
  paid_search Int,
  partner_plugins String,
  persistent_cookie String,
  plugins String,
  pointofinterest String,
  pointofinterestdistance String,
  post_browser_height Int,
  post_browser_width Int,
  post_campaign String,
  post_channel String,
  post_cookies String,
  post_currency String,
  post_cust_hit_time_gmt BigInt,
  post_cust_visid Int,
  post_ef_id String,
  post_evar1 String,
  post_evar2 String,
  post_evar3 String,
  post_evar4 String,
  post_evar5 String,
  post_evar6 String,
  post_evar7 String,
  post_evar8 String,
  post_evar9 String,
  post_evar10 String,
  post_evar11 String,
  post_evar12 String,
  post_evar13 String,
  post_evar14 String,
  post_evar15 String,
  post_evar16 String,
  post_evar17 String,
  post_evar18 String,
  post_evar19 String,
  post_evar20 String,
  post_evar21 String,
  post_evar22 String,
  post_evar23 String,
  post_evar24 String,
  post_evar25 String,
  post_evar26 String,
  post_evar27 String,
  post_evar28 String,
  post_evar29 String,
  post_evar30 String,
  post_evar31 String,
  post_evar32 String,
  post_evar33 String,
  post_evar34 String,
  post_evar35 String,
  post_evar36 String,
  post_evar37 String,
  post_evar38 String,
  post_evar39 String,
  post_evar40 String,
  post_evar41 String,
  post_evar42 String,
  post_evar43 String,
  post_evar44 String,
  post_evar45 String,
  post_evar46 String,
  post_evar47 String,
  post_evar48 String,
  post_evar49 String,
  post_evar50 String,
  post_evar51 String,
  post_evar52 String,
  post_evar53 String,
  post_evar54 String,
  post_evar55 String,
  post_evar56 String,
  post_evar57 String,
  post_evar58 String,
  post_evar59 String,
  post_evar60 String,
  post_evar61 String,
  post_evar62 String,
  post_evar63 String,
  post_evar64 String,
  post_evar65 String,
  post_evar66 String,
  post_evar67 String,
  post_evar68 String,
  post_evar69 String,
  post_evar70 String,
  post_evar71 String,
  post_evar72 String,
  post_evar73 String,
  post_evar74 String,
  post_evar75 String,
  post_evar76 String,
  post_evar77 String,
  post_evar78 String,
  post_evar79 String,
  post_evar80 String,
  post_evar81 String,
  post_evar82 String,
  post_evar83 String,
  post_evar84 String,
  post_evar85 String,
  post_evar86 String,
  post_evar87 String,
  post_evar88 String,
  post_evar89 String,
  post_evar90 String,
  post_evar91 String,
  post_evar92 String,
  post_evar93 String,
  post_evar94 String,
  post_evar95 String,
  post_evar96 String,
  post_evar97 String,
  post_evar98 String,
  post_evar99 String,
  post_evar100 String,
  post_event_list String,
  post_hier1 String,
  post_hier2 String,
  post_hier3 String,
  post_hier4 String,
  post_hier5 String,
  post_java_enabled String,
  post_keywords String,
  post_mc_audiences String,
  post_mobileaction String,
  post_mobileappid String,
  post_mobilecampaigncontent String,
  post_mobilecampaignmedium String,
  post_mobilecampaignname String,
  post_mobilecampaignsource String,
  post_mobilecampaignterm String,
  post_mobiledayofweek String,
  post_mobiledayssincefirstuse String,
  post_mobiledayssincelastuse String,
  post_mobiledevice String,
  post_mobilehourofday String,
  post_mobileinstalldate String,
  post_mobilelaunchnumber String,
  post_mobileltv String,
  post_mobilemessageid String,
  post_mobilemessageonline String,
  post_mobileosversion String,
  post_mobilepushoptin String,
  post_mobilepushpayloadid String,
  post_mobileresolution String,
  post_mvvar1 String,
  post_mvvar2 String,
  post_mvvar3 String,
  post_page_event Int,
  post_page_event_var1 String,
  post_page_event_var2 String,
  post_page_event_var3 String,
  post_page_type String,
  post_page_url String,
  post_pagename String,
  post_pagename_no_url String,
  post_partner_plugins String,
  post_persistent_cookie String,
  post_pointofinterest String,
  post_pointofinterestdistance String,
  post_product_list String,
  post_prop1 String,
  post_prop2 String,
  post_prop3 String,
  post_prop4 String,
  post_prop5 String,
  post_prop6 String,
  post_prop7 String,
  post_prop8 String,
  post_prop9 String,
  post_prop10 String,
  post_prop11 String,
  post_prop12 String,
  post_prop13 String,
  post_prop14 String,
  post_prop15 String,
  post_prop16 String,
  post_prop17 String,
  post_prop18 String,
  post_prop19 String,
  post_prop20 String,
  post_prop21 String,
  post_prop22 String,
  post_prop23 String,
  post_prop24 String,
  post_prop25 String,
  post_prop26 String,
  post_prop27 String,
  post_prop28 String,
  post_prop29 String,
  post_prop30 String,
  post_prop31 String,
  post_prop32 String,
  post_prop33 String,
  post_prop34 String,
  post_prop35 String,
  post_prop36 String,
  post_prop37 String,
  post_prop38 String,
  post_prop39 String,
  post_prop40 String,
  post_prop41 String,
  post_prop42 String,
  post_prop43 String,
  post_prop44 String,
  post_prop45 String,
  post_prop46 String,
  post_prop47 String,
  post_prop48 String,
  post_prop49 String,
  post_prop50 String,
  post_prop51 String,
  post_prop52 String,
  post_prop53 String,
  post_prop54 String,
  post_prop55 String,
  post_prop56 String,
  post_prop57 String,
  post_prop58 String,
  post_prop59 String,
  post_prop60 String,
  post_prop61 String,
  post_prop62 String,
  post_prop63 String,
  post_prop64 String,
  post_prop65 String,
  post_prop66 String,
  post_prop67 String,
  post_prop68 String,
  post_prop69 String,
  post_prop70 String,
  post_prop71 String,
  post_prop72 String,
  post_prop73 String,
  post_prop74 String,
  post_prop75 String,
  post_purchaseid String,
  post_referrer String,
  post_s_kwcid String,
  post_search_engine Int,
  post_socialaccountandappids String,
  post_socialassettrackingcode String,
  post_socialauthor String,
  post_socialcontentprovider String,
  post_socialfbstories String,
  post_socialfbstorytellers String,
  post_socialinteractioncount String,
  post_socialinteractiontype String,
  post_sociallanguage String,
  post_sociallatlong String,
  post_sociallikeadds String,
  post_socialmentions String,
  post_socialowneddefinitioninsighttype String,
  post_socialowneddefinitioninsightvalue String,
  post_socialowneddefinitionmetric String,
  post_socialowneddefinitionpropertyvspost String,
  post_socialownedpostids String,
  post_socialownedpropertyid String,
  post_socialownedpropertyname String,
  post_socialownedpropertypropertyvsapp String,
  post_socialpageviews String,
  post_socialpostviews String,
  post_socialpubcomments String,
  post_socialpubposts String,
  post_socialpubrecommends String,
  post_socialpubsubscribers String,
  post_socialterm String,
  post_socialtotalsentiment String,
  post_state String,
  post_survey String,
  post_t_time_info String,
  post_tnt String,
  post_tnt_action String,
  post_transactionid String,
  post_video String,
  post_videoad String,
  post_videoadinpod String,
  post_videoadplayername String,
  post_videoadpod String,
  post_videochannel String,
  post_videochapter String,
  post_videocontenttype String,
  post_videoplayername String,
  post_videoqoebitrateaverageevar String,
  post_videoqoebitratechangecountevar String,
  post_videoqoebuffercountevar String,
  post_videoqoebuffertimeevar String,
  post_videoqoedroppedframecountevar String,
  post_videoqoeerrorcountevar String,
  post_videoqoetimetostartevar String,
  post_videosegment String,
  post_visid_high BigInt,
  post_visid_low BigInt,
  post_visid_type Int,
  post_zip String,
  prev_page Int,
  product_list String,
  product_merchandising String,
  prop1 String,
  prop2 String,
  prop3 String,
  prop4 String,
  prop5 String,
  prop6 String,
  prop7 String,
  prop8 String,
  prop9 String,
  prop10 String,
  prop11 String,
  prop12 String,
  prop13 String,
  prop14 String,
  prop15 String,
  prop16 String,
  prop17 String,
  prop18 String,
  prop19 String,
  prop20 String,
  prop21 String,
  prop22 String,
  prop23 String,
  prop24 String,
  prop25 String,
  prop26 String,
  prop27 String,
  prop28 String,
  prop29 String,
  prop30 String,
  prop31 String,
  prop32 String,
  prop33 String,
  prop34 String,
  prop35 String,
  prop36 String,
  prop37 String,
  prop38 String,
  prop39 String,
  prop40 String,
  prop41 String,
  prop42 String,
  prop43 String,
  prop44 String,
  prop45 String,
  prop46 String,
  prop47 String,
  prop48 String,
  prop49 String,
  prop50 String,
  prop51 String,
  prop52 String,
  prop53 String,
  prop54 String,
  prop55 String,
  prop56 String,
  prop57 String,
  prop58 String,
  prop59 String,
  prop60 String,
  prop61 String,
  prop62 String,
  prop63 String,
  prop64 String,
  prop65 String,
  prop66 String,
  prop67 String,
  prop68 String,
  prop69 String,
  prop70 String,
  prop71 String,
  prop72 String,
  prop73 String,
  prop74 String,
  prop75 String,
  purchaseid String,
  quarterly_visitor Int,
  ref_domain String,
  ref_type Int,
  referrer String,
  resolution Int,
  s_kwcid String,
  s_resolution String,
  sampled_hit String,
  search_engine Int,
  search_page_num Int,
  secondary_hit Int,
  service String,
  socialaccountandappids String,
  socialassettrackingcode String,
  socialauthor String,
  socialcontentprovider String,
  socialfbstories String,
  socialfbstorytellers String,
  socialinteractioncount String,
  socialinteractiontype String,
  sociallanguage String,
  sociallatlong String,
  sociallikeadds String,
  socialmentions String,
  socialowneddefinitioninsighttype String,
  socialowneddefinitioninsightvalue String,
  socialowneddefinitionmetric String,
  socialowneddefinitionpropertyvspost String,
  socialownedpostids String,
  socialownedpropertyid String,
  socialownedpropertyname String,
  socialownedpropertypropertyvsapp String,
  socialpageviews String,
  socialpostviews String,
  socialpubcomments String,
  socialpubposts String,
  socialpubrecommends String,
  socialpubsubscribers String,
  socialterm String,
  socialtotalsentiment String,
  sourceid Int,
  state String,
  stats_server String,
  t_time_info String,
  tnt String,
  tnt_action String,
  tnt_post_vista String,
  transactionid String,
  truncated_hit String,
  ua_color String,
  ua_os String,
  ua_pixels String,
  user_agent String,
  user_hash Int,
  user_server String,
  userid Int,
  username String,
  va_closer_detail Int,
  va_closer_id String,
  va_finder_detail String,
  va_finder_id Int,
  va_instance_event Int,
  va_new_engagement Int,
  video String,
  videoad String,
  videoadinpod String,
  videoadplayername String,
  videoadpod String,
  videochannel String,
  videochapter String,
  videocontenttype String,
  videoplayername String,
  videoqoebitrateaverageevar String,
  videoqoebitratechangecountevar String,
  videoqoebuffercountevar String,
  videoqoebuffertimeevar String,
  videoqoedroppedframecountevar String,
  videoqoeerrorcountevar String,
  videoqoetimetostartevar String,
  videosegment String,
  visid_high BigInt,
  visid_low BigInt,
  visid_new String,
  visid_timestamp BigInt,
  visid_type Int,
  visit_keywords String,
  visit_num Int,
  visit_page_num Int,
  visit_referrer String,
  visit_search_engine Int,
  visit_start_page_url String,
  visit_start_pagename String,
  visit_start_time_gmt BigInt,
  weekly_visitor Int,
  yearly_visitor Int,
  zip BigInt
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/apps/hive/warehouse/${env:TMP_db}.db/nifi_sbnet_raw'
TBLPROPERTIES('serialization.null.format'='');