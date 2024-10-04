set job.priority HIGH;
set output.compression.enabled true;
set mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
set pig.exec.mapPartAgg true; -- for optimizing the group by statements
set pig.cachedbag.memusage 0.35; -- increased the memory usage of the pig script by 15%. Previously it was 20% and now it is 35%

REGISTER 'hdfs:///udf/charter-pig-udf-0.1-SNAPSHOT.jar';
DEFINE aes_encrypt com.charter.hadoop.pig.udf.PigAesEncrypt256();

raw_sbnet_bhn = LOAD '$TMP_db.sbnet_bhn_raw' USING org.apache.hive.hcatalog.pig.HCatLoader();

-- loading of the tables
--Following tables are adobe generated
map_browser = LOAD '$LKP_db.sbnet_bhn_browser' USING org.apache.hive.hcatalog.pig.HCatLoader();
map_country = LOAD '$LKP_db.sbnet_bhn_country' USING org.apache.hive.hcatalog.pig.HCatLoader();
map_connection_type = LOAD '$LKP_db.sbnet_bhn_connection_type' USING org.apache.hive.hcatalog.pig.HCatLoader();
map_os = LOAD '$LKP_db.sbnet_bhn_os' USING org.apache.hive.hcatalog.pig.HCatLoader();
map_resolution = LOAD '$LKP_db.sbnet_bhn_resolution' USING org.apache.hive.hcatalog.pig.HCatLoader();

map_browser = DISTINCT map_browser;
map_country = DISTINCT map_country;
map_connection_type = DISTINCT map_connection_type;
map_os = DISTINCT map_os;
map_resolution = DISTINCT map_resolution;

-- JOINs to create final relation for denormalization
raw_sbnet_bhn = JOIN raw_sbnet_bhn BY browser LEFT OUTER, map_browser BY browser_id  USING 'replicated';
raw_sbnet_bhn = JOIN raw_sbnet_bhn BY country LEFT OUTER, map_country BY country_id  USING 'replicated';
raw_sbnet_bhn = JOIN raw_sbnet_bhn BY connection_type LEFT OUTER,  map_connection_type BY connection_type_id  USING 'replicated';
raw_sbnet_bhn = JOIN raw_sbnet_bhn BY os LEFT OUTER, map_os BY os_id  USING 'replicated';
raw_sbnet_bhn = JOIN raw_sbnet_bhn BY resolution LEFT OUTER, map_resolution BY resolution_id  USING 'replicated';

raw_sbnet_bhn = rank raw_sbnet_bhn;

sbnet_bhn_denorm = FOREACH raw_sbnet_bhn GENERATE
  UniqueID() AS unique_id:chararray,
  (long)$0 AS rank_id:long,
  accept_language AS accept_language:chararray,
  map_browser::detail AS browser:chararray,
  browser_height AS browser_height:int,
  browser_width AS browser_width:int,
  campaign AS campaign:chararray,
  c_color AS c_color:chararray,
  channel AS channel:chararray,
  click_action AS click_action:chararray,
  click_action_type AS click_action_type:int,
  click_context AS click_context:chararray,
  click_context_type AS click_context_type:int,
  click_sourceid AS click_sourceid:int,
  click_tag AS click_tag:chararray,
  code_ver AS code_ver:chararray,
  color AS color:int,
  map_connection_type::detail AS connection_type:chararray,
  cookies AS cookies:chararray,
  map_country::detail AS country:chararray,
  ct_connect_type AS ct_connect_type:chararray,
  currency AS currency:chararray,
  curr_factor AS curr_factor:int,
  curr_rate AS curr_rate:double,
  cust_hit_time_gmt AS cust_hit_time_gmt:long,
  cust_visid AS cust_visid:chararray,
  daily_visitor AS daily_visitor:int,
  date_time AS date_time:chararray,
  domain AS domain:chararray,
  duplicated_from AS duplicated_from:chararray,
  duplicate_events AS duplicate_events:chararray,
  duplicate_purchase AS duplicate_purchase:int,
  event_list AS event_list:chararray,
  exclude_hit AS exclude_hit:int,
  first_hit_pagename AS first_hit_pagename:chararray,
  first_hit_page_url AS first_hit_page_url:chararray,
  first_hit_referrer AS first_hit_referrer:chararray,
  first_hit_time_gmt AS first_hit_time_gmt:long,
  geo_city AS geo_city:chararray,
  geo_country AS geo_country:chararray,
  geo_dma AS geo_dma:int,
  geo_region AS geo_region:chararray,
  geo_zip AS geo_zip:chararray,
  hier1 AS hier1:chararray,
  hier2 AS hier2:chararray,
  hier3 AS hier3:chararray,
  hier4 AS hier4:chararray,
  hier5 AS hier5:chararray,
  hitid_high AS hitid_high:long,
  hitid_low AS hitid_low:long,
  hit_source AS hit_source:int,
  hit_time_gmt AS hit_time_gmt:long,
  homepage AS homepage:chararray,
  hourly_visitor AS hourly_visitor:int,
  (ip is not NULL ? aes_encrypt(ip) : ip) AS ip:chararray,
  ip2 AS ip2:chararray,
  java_enabled AS java_enabled:chararray,
  javascript AS javascript:int,
  j_jscript AS j_jscript:chararray,
  language AS language:int,
  last_hit_time_gmt AS last_hit_time_gmt:long,
  last_purchase_num AS last_purchase_num:int,
  last_purchase_time_gmt AS last_purchase_time_gmt:long,
  mobile_id AS mobile_id:int,
  monthly_visitor AS monthly_visitor:int,
  mvvar1 AS mvvar1:chararray,
  mvvar2 AS mvvar2:chararray,
  mvvar3 AS mvvar3:chararray,
  namespace AS namespace:chararray,
  new_visit AS new_visit:int,
  map_os::detail AS os:chararray,
  page_event AS page_event:int,
  page_event_var1 AS page_event_var1:chararray,
  page_event_var2 AS page_event_var2:chararray,
  page_event_var3 AS page_event_var3:chararray,
  pagename AS pagename:chararray,
  page_type AS page_type:chararray,
  page_url AS page_url:chararray,
  paid_search AS paid_search:int,
  partner_plugins AS partner_plugins:chararray,
  persistent_cookie AS persistent_cookie:chararray,
  plugins AS plugins:chararray,
  post_browser_height AS post_browser_height:int,
  post_browser_width AS post_browser_width:int,
  post_campaign AS post_campaign:chararray,
  post_channel AS post_channel:chararray,
  post_cookies AS post_cookies:chararray,
  post_currency AS post_currency:chararray,
  post_cust_hit_time_gmt AS post_cust_hit_time_gmt:long,
  post_cust_visid AS post_cust_visid:int,
  post_evar1 AS post_evar1:chararray,
  post_evar2 AS post_evar2:chararray,
  post_evar3 AS post_evar3:chararray,
  post_evar4 AS post_evar4:chararray,
  post_evar5 AS post_evar5:chararray,
  post_evar6 AS post_evar6:chararray,
  post_evar7 AS post_evar7:chararray,
  post_evar8 AS post_evar8:chararray,
  post_evar9 AS post_evar9:chararray,
  post_evar10 AS post_evar10:chararray,
  post_evar11 AS post_evar11:chararray,
  post_evar12 AS post_evar12:chararray,
  post_evar13 AS post_evar13:chararray,
  post_evar14 AS post_evar14:chararray,
  post_evar15 AS post_evar15:chararray,
  post_evar16 AS post_evar16:chararray,
  post_evar17 AS post_evar17:chararray,
  post_evar18 AS post_evar18:chararray,
  post_evar19 AS post_evar19:chararray,
  post_evar20 AS post_evar20:chararray,
  post_evar21 AS post_evar21:chararray,
  post_evar22 AS post_evar22:chararray,
  post_evar23 AS post_evar23:chararray,
  post_evar24 AS post_evar24:chararray,
  post_evar25 AS post_evar25:chararray,
  post_evar26 AS post_evar26:chararray,
  post_evar27 AS post_evar27:chararray,
  post_evar28 AS post_evar28:chararray,
  post_evar29 AS post_evar29:chararray,
  post_evar30 AS post_evar30:chararray,
  post_evar31 AS post_evar31:chararray,
  post_evar32 AS post_evar32:chararray,
  post_evar33 AS post_evar33:chararray,
  post_evar34 AS post_evar34:chararray,
  post_evar35 AS post_evar35:chararray,
  post_evar36 AS post_evar36:chararray,
  post_evar37 AS post_evar37:chararray,
  post_evar38 AS post_evar38:chararray,
  post_evar39 AS post_evar39:chararray,
  post_evar40 AS post_evar40:chararray,
  post_evar41 AS post_evar41:chararray,
  post_evar42 AS post_evar42:chararray,
  post_evar43 AS post_evar43:chararray,
  post_evar44 AS post_evar44:chararray,
  post_evar45 AS post_evar45:chararray,
  post_evar46 AS post_evar46:chararray,
  post_evar47 AS post_evar47:chararray,
  post_evar48 AS post_evar48:chararray,
  post_evar49 AS post_evar49:chararray,
  post_evar50 AS post_evar50:chararray,
  post_evar51 AS post_evar51:chararray,
  post_evar52 AS post_evar52:chararray,
  post_evar53 AS post_evar53:chararray,
  post_evar54 AS post_evar54:chararray,
  post_evar55 AS post_evar55:chararray,
  post_evar56 AS post_evar56:chararray,
  post_evar57 AS post_evar57:chararray,
  post_evar58 AS post_evar58:chararray,
  post_evar59 AS post_evar59:chararray,
  post_evar60 AS post_evar60:chararray,
  post_evar61 AS post_evar61:chararray,
  post_evar62 AS post_evar62:chararray,
  post_evar63 AS post_evar63:chararray,
  post_evar64 AS post_evar64:chararray,
  post_evar65 AS post_evar65:chararray,
  post_evar66 AS post_evar66:chararray,
  post_evar67 AS post_evar67:chararray,
  post_evar68 AS post_evar68:chararray,
  post_evar69 AS post_evar69:chararray,
  post_evar70 AS post_evar70:chararray,
  post_evar71 AS post_evar71:chararray,
  post_evar72 AS post_evar72:chararray,
  post_evar73 AS post_evar73:chararray,
  post_evar74 AS post_evar74:chararray,
  post_evar75 AS post_evar75:chararray,
  post_event_list AS post_event_list:chararray,
  post_hier1 AS post_hier1:chararray,
  post_hier2 AS post_hier2:chararray,
  post_hier3 AS post_hier3:chararray,
  post_hier4 AS post_hier4:chararray,
  post_hier5 AS post_hier5:chararray,
  post_java_enabled AS post_java_enabled:chararray,
  post_keywords AS post_keywords:chararray,
  post_mvvar1 AS post_mvvar1:chararray,
  post_mvvar2 AS post_mvvar2:chararray,
  post_mvvar3 AS post_mvvar3:chararray,
  post_page_event AS post_page_event:int,
  post_page_event_var1 AS post_page_event_var1:chararray,
  post_page_event_var2 AS post_page_event_var2:chararray,
  post_page_event_var3 AS post_page_event_var3:chararray,
  post_pagename AS post_pagename:chararray,
  post_pagename_no_url AS post_pagename_no_url:chararray,
  post_page_type AS post_page_type:chararray,
  post_page_url AS post_page_url:chararray,
  post_partner_plugins AS post_partner_plugins:chararray,
  post_persistent_cookie AS post_persistent_cookie:chararray,
  post_product_list AS post_product_list:chararray,
  post_prop1 AS post_prop1:chararray,
  post_prop2 AS post_prop2:chararray,
  post_prop3 AS post_prop3:chararray,
  post_prop4 AS post_prop4:chararray,
  post_prop5 AS post_prop5:chararray,
  post_prop6 AS post_prop6:chararray,
  post_prop7 AS post_prop7:chararray,
  post_prop8 AS post_prop8:chararray,
  post_prop9 AS post_prop9:chararray,
  post_prop10 AS post_prop10:chararray,
  post_prop11 AS post_prop11:chararray,
  post_prop12 AS post_prop12:chararray,
  post_prop13 AS post_prop13:chararray,
  post_prop14 AS post_prop14:chararray,
  post_prop15 AS post_prop15:chararray,
  post_prop16 AS post_prop16:chararray,
  post_prop17 AS post_prop17:chararray,
  post_prop18 AS post_prop18:chararray,
  post_prop19 AS post_prop19:chararray,
  post_prop20 AS post_prop20:chararray,
  post_prop21 AS post_prop21:chararray,
  post_prop22 AS post_prop22:chararray,
  post_prop23 AS post_prop23:chararray,
  post_prop24 AS post_prop24:chararray,
  post_prop25 AS post_prop25:chararray,
  post_prop26 AS post_prop26:chararray,
  post_prop27 AS post_prop27:chararray,
  post_prop28 AS post_prop28:chararray,
  post_prop29 AS post_prop29:chararray,
  post_prop30 AS post_prop30:chararray,
  post_prop31 AS post_prop31:chararray,
  post_prop32 AS post_prop32:chararray,
  post_prop33 AS post_prop33:chararray,
  post_prop34 AS post_prop34:chararray,
  post_prop35 AS post_prop35:chararray,
  post_prop36 AS post_prop36:chararray,
  post_prop37 AS post_prop37:chararray,
  post_prop38 AS post_prop38:chararray,
  post_prop39 AS post_prop39:chararray,
  post_prop40 AS post_prop40:chararray,
  post_prop41 AS post_prop41:chararray,
  post_prop42 AS post_prop42:chararray,
  post_prop43 AS post_prop43:chararray,
  post_prop44 AS post_prop44:chararray,
  post_prop45 AS post_prop45:chararray,
  post_prop46 AS post_prop46:chararray,
  post_prop47 AS post_prop47:chararray,
  post_prop48 AS post_prop48:chararray,
  post_prop49 AS post_prop49:chararray,
  post_prop50 AS post_prop50:chararray,
  post_prop51 AS post_prop51:chararray,
  post_prop52 AS post_prop52:chararray,
  post_prop53 AS post_prop53:chararray,
  post_prop54 AS post_prop54:chararray,
  post_prop55 AS post_prop55:chararray,
  post_prop56 AS post_prop56:chararray,
  post_prop57 AS post_prop57:chararray,
  post_prop58 AS post_prop58:chararray,
  post_prop59 AS post_prop59:chararray,
  post_prop60 AS post_prop60:chararray,
  post_prop61 AS post_prop61:chararray,
  post_prop62 AS post_prop62:chararray,
  post_prop63 AS post_prop63:chararray,
  post_prop64 AS post_prop64:chararray,
  post_prop65 AS post_prop65:chararray,
  post_prop66 AS post_prop66:chararray,
  post_prop67 AS post_prop67:chararray,
  post_prop68 AS post_prop68:chararray,
  post_prop69 AS post_prop69:chararray,
  post_prop70 AS post_prop70:chararray,
  post_prop71 AS post_prop71:chararray,
  post_prop72 AS post_prop72:chararray,
  post_prop73 AS post_prop73:chararray,
  post_prop74 AS post_prop74:chararray,
  post_prop75 AS post_prop75:chararray,
  post_purchaseid AS post_purchaseid:chararray,
  post_referrer AS post_referrer:chararray,
  post_search_engine AS post_search_engine:int,
  post_state AS post_state:chararray,
  post_survey AS post_survey:chararray,
  post_tnt AS post_tnt:chararray,
  post_transactionid AS post_transactionid:chararray,
  post_t_time_info AS post_t_time_info:chararray,
  post_visid_high AS post_visid_high:long,
  post_visid_low AS post_visid_low:long,
  post_visid_type AS post_visid_type:int,
  post_zip AS post_zip:chararray,
  p_plugins AS p_plugins:chararray,
  prev_page AS prev_page:int,
  product_list AS product_list:chararray,
  product_merchandising AS product_merchandising:chararray,
  purchaseid AS purchaseid:chararray,
  quarterly_visitor AS quarterly_visitor:int,
  ref_domain AS ref_domain:chararray,
  referrer AS referrer:chararray,
  ref_type AS ref_type:int,
  map_resolution::detail AS resolution:chararray,
  sampled_hit AS sampled_hit:chararray,
  search_engine AS search_engine:int,
  search_page_num AS search_page_num:int,
  secondary_hit AS secondary_hit:int,
  service AS service:chararray,
  sourceid AS sourceid:int,
  s_resolution AS s_resolution:chararray,
  state AS state:chararray,
  stats_server AS stats_server:chararray,
  tnt AS tnt:chararray,
  tnt_post_vista AS tnt_post_vista:chararray,
  transactionid AS transactionid:chararray,
  truncated_hit AS truncated_hit:chararray,
  t_time_info AS t_time_info:chararray,
  ua_color AS ua_color:chararray,
  ua_os AS ua_os:chararray,
  ua_pixels AS ua_pixels:chararray,
  user_agent AS user_agent:chararray,
  user_hash AS user_hash:int,
  userid AS userid:int,
  username AS username:chararray,
  user_server AS user_server:chararray,
  va_closer_detail AS va_closer_detail:int,
  va_closer_id AS va_closer_id:chararray,
  va_finder_detail AS va_finder_detail:chararray,
  va_finder_id AS va_finder_id:int,
  va_instance_event AS va_instance_event:int,
  va_new_engagement AS va_new_engagement:int,
  visid_high AS visid_high:long,
  visid_low AS visid_low:long,
  visid_new AS visid_new:chararray,
  visid_timestamp AS visid_timestamp:long,
  visid_type AS visid_type:int,
  visit_keywords AS visit_keywords:chararray,
  visit_num AS visit_num:int,
  visit_page_num AS visit_page_num:int,
  visit_referrer AS visit_referrer:chararray,
  visit_search_engine AS visit_search_engine:int,
  visit_start_pagename AS visit_start_pagename:chararray,
  visit_start_page_url AS visit_start_page_url:chararray,
  visit_start_time_gmt AS visit_start_time_gmt:long,
  weekly_visitor AS weekly_visitor:int,
  yearly_visitor AS yearly_visitor:int,
  zip AS zip:long;


STORE sbnet_bhn_denorm INTO '$TMP_db.sbnet_bhn_denorm' USING org.apache.hive.hcatalog.pig.HCatStorer();
