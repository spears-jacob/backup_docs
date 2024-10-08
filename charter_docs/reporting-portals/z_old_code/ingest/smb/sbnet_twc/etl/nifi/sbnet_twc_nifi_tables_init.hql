USE ${env:LKP_db}
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_twc_event
(
  event_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_twc_browser
(
  browser_id string,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_twc_country
(
  country_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_twc_connection_type
(
  connection_type_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_twc_javascript
(
  javascript_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_twc_language
(
  language_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_twc_os
(
  os_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_twc_resolution
(
  resolution_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS nifi_sbnet_twc_search_engine
(
  search_engine_id int,
  detail string
) 
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE EXTERNAL TABLE IF NOT EXISTS ${env:TMP_db}.nifi_sbnet_twc_raw
(
  accept_language string,
  aemassetid string,
  aemassetsource string,
  aemclickedassetid string,
  browser string,
  browser_height int,
  browser_width int,
  c_color string,
  campaign string,
  carrier string,
  channel string,
  click_action string,
  click_action_type int,
  click_context string,
  click_context_type int,
  click_sourceid int,
  click_tag string,
  code_ver string,
  color int,
  connection_type int,
  cookies string,
  country int,
  ct_connect_type string,
  curr_factor int,
  curr_rate double,
  currency string,
  cust_hit_time_gmt bigint,
  cust_visid string,
  daily_visitor int,
  date_time string,
  domain string,
  duplicate_events string,
  duplicate_purchase int,
  duplicated_from string,
  ef_id string,
  evar1 string,
  evar10 string,
  evar100 string,
  evar101 string,
  evar102 string,
  evar103 string,
  evar104 string,
  evar105 string,
  evar106 string,
  evar107 string,
  evar108 string,
  evar109 string,
  evar11 string,
  evar110 string,
  evar111 string,
  evar112 string,
  evar113 string,
  evar114 string,
  evar115 string,
  evar116 string,
  evar117 string,
  evar118 string,
  evar119 string,
  evar12 string,
  evar120 string,
  evar121 string,
  evar122 string,
  evar123 string,
  evar124 string,
  evar125 string,
  evar126 string,
  evar127 string,
  evar128 string,
  evar129 string,
  evar13 string,
  evar130 string,
  evar131 string,
  evar132 string,
  evar133 string,
  evar134 string,
  evar135 string,
  evar136 string,
  evar137 string,
  evar138 string,
  evar139 string,
  evar14 string,
  evar140 string,
  evar141 string,
  evar142 string,
  evar143 string,
  evar144 string,
  evar145 string,
  evar146 string,
  evar147 string,
  evar148 string,
  evar149 string,
  evar15 string,
  evar150 string,
  evar151 string,
  evar152 string,
  evar153 string,
  evar154 string,
  evar155 string,
  evar156 string,
  evar157 string,
  evar158 string,
  evar159 string,
  evar16 string,
  evar160 string,
  evar161 string,
  evar162 string,
  evar163 string,
  evar164 string,
  evar165 string,
  evar166 string,
  evar167 string,
  evar168 string,
  evar169 string,
  evar17 string,
  evar170 string,
  evar171 string,
  evar172 string,
  evar173 string,
  evar174 string,
  evar175 string,
  evar176 string,
  evar177 string,
  evar178 string,
  evar179 string,
  evar18 string,
  evar180 string,
  evar181 string,
  evar182 string,
  evar183 string,
  evar184 string,
  evar185 string,
  evar186 string,
  evar187 string,
  evar188 string,
  evar189 string,
  evar19 string,
  evar190 string,
  evar191 string,
  evar192 string,
  evar193 string,
  evar194 string,
  evar195 string,
  evar196 string,
  evar197 string,
  evar198 string,
  evar199 string,
  evar2 string,
  evar20 string,
  evar200 string,
  evar201 string,
  evar202 string,
  evar203 string,
  evar204 string,
  evar205 string,
  evar206 string,
  evar207 string,
  evar208 string,
  evar209 string,
  evar21 string,
  evar210 string,
  evar211 string,
  evar212 string,
  evar213 string,
  evar214 string,
  evar215 string,
  evar216 string,
  evar217 string,
  evar218 string,
  evar219 string,
  evar22 string,
  evar220 string,
  evar221 string,
  evar222 string,
  evar223 string,
  evar224 string,
  evar225 string,
  evar226 string,
  evar227 string,
  evar228 string,
  evar229 string,
  evar23 string,
  evar230 string,
  evar231 string,
  evar232 string,
  evar233 string,
  evar234 string,
  evar235 string,
  evar236 string,
  evar237 string,
  evar238 string,
  evar239 string,
  evar24 string,
  evar240 string,
  evar241 string,
  evar242 string,
  evar243 string,
  evar244 string,
  evar245 string,
  evar246 string,
  evar247 string,
  evar248 string,
  evar249 string,
  evar25 string,
  evar250 string,
  evar26 string,
  evar27 string,
  evar28 string,
  evar29 string,
  evar3 string,
  evar30 string,
  evar31 string,
  evar32 string,
  evar33 string,
  evar34 string,
  evar35 string,
  evar36 string,
  evar37 string,
  evar38 string,
  evar39 string,
  evar4 string,
  evar40 string,
  evar41 string,
  evar42 string,
  evar43 string,
  evar44 string,
  evar45 string,
  evar46 string,
  evar47 string,
  evar48 string,
  evar49 string,
  evar5 string,
  evar50 string,
  evar51 string,
  evar52 string,
  evar53 string,
  evar54 string,
  evar55 string,
  evar56 string,
  evar57 string,
  evar58 string,
  evar59 string,
  evar6 string,
  evar60 string,
  evar61 string,
  evar62 string,
  evar63 string,
  evar64 string,
  evar65 string,
  evar66 string,
  evar67 string,
  evar68 string,
  evar69 string,
  evar7 string,
  evar70 string,
  evar71 string,
  evar72 string,
  evar73 string,
  evar74 string,
  evar75 string,
  evar76 string,
  evar77 string,
  evar78 string,
  evar79 string,
  evar8 string,
  evar80 string,
  evar81 string,
  evar82 string,
  evar83 string,
  evar84 string,
  evar85 string,
  evar86 string,
  evar87 string,
  evar88 string,
  evar89 string,
  evar9 string,
  evar90 string,
  evar91 string,
  evar92 string,
  evar93 string,
  evar94 string,
  evar95 string,
  evar96 string,
  evar97 string,
  evar98 string,
  evar99 string,
  event_list string,
  exclude_hit int,
  first_hit_page_url string,
  first_hit_pagename string,
  first_hit_ref_domain string,
  first_hit_ref_type string,
  first_hit_referrer string,
  first_hit_time_gmt bigint,
  geo_city string,
  geo_country string,
  geo_dma int,
  geo_region string,
  geo_zip string,
  hier1 string,
  hier2 string,
  hier3 string,
  hier4 string,
  hier5 string,
  hit_source int,
  hit_time_gmt bigint,
  hitid_high bigint,
  hitid_low bigint,
  homepage string,
  hourly_visitor int,
  ip string,
  ip2 string,
  j_jscript string,
  java_enabled string,
  javascript int,
  language int,
  last_hit_time_gmt bigint,
  last_purchase_num int,
  last_purchase_time_gmt bigint,
  mc_audiences string,
  mcvisid string,
  mobile_id int,
  mobileaction string,
  mobileappid string,
  mobilecampaigncontent string,
  mobilecampaignmedium string,
  mobilecampaignname string,
  mobilecampaignsource string,
  mobilecampaignterm string,
  mobiledayofweek string,
  mobiledayssincefirstuse string,
  mobiledayssincelastuse string,
  mobiledevice string,
  mobilehourofday string,
  mobileinstalldate string,
  mobilelaunchnumber string,
  mobileltv string,
  mobilemessageid string,
  mobilemessageonline string,
  mobileosversion string,
  mobilepushoptin string,
  mobilepushpayloadid string,
  mobileresolution string,
  monthly_visitor int,
  mvvar1 string,
  mvvar2 string,
  mvvar3 string,
  namespace string,
  new_visit int,
  os int,
  p_plugins string,
  page_event int,
  page_event_var1 string,
  page_event_var2 string,
  page_event_var3 string,
  page_type string,
  page_url string,
  pagename string,
  paid_search int,
  partner_plugins string,
  persistent_cookie string,
  plugins string,
  pointofinterest string,
  pointofinterestdistance string,
  post_browser_height int,
  post_browser_width int,
  post_campaign string,
  post_channel string,
  post_cookies string,
  post_currency string,
  post_cust_hit_time_gmt bigint,
  post_cust_visid int,
  post_ef_id string,
  post_evar1 string,
  post_evar10 string,
  post_evar100 string,
  post_evar101 string,
  post_evar102 string,
  post_evar103 string,
  post_evar104 string,
  post_evar105 string,
  post_evar106 string,
  post_evar107 string,
  post_evar108 string,
  post_evar109 string,
  post_evar11 string,
  post_evar110 string,
  post_evar111 string,
  post_evar112 string,
  post_evar113 string,
  post_evar114 string,
  post_evar115 string,
  post_evar116 string,
  post_evar117 string,
  post_evar118 string,
  post_evar119 string,
  post_evar12 string,
  post_evar120 string,
  post_evar121 string,
  post_evar122 string,
  post_evar123 string,
  post_evar124 string,
  post_evar125 string,
  post_evar126 string,
  post_evar127 string,
  post_evar128 string,
  post_evar129 string,
  post_evar13 string,
  post_evar130 string,
  post_evar131 string,
  post_evar132 string,
  post_evar133 string,
  post_evar134 string,
  post_evar135 string,
  post_evar136 string,
  post_evar137 string,
  post_evar138 string,
  post_evar139 string,
  post_evar14 string,
  post_evar140 string,
  post_evar141 string,
  post_evar142 string,
  post_evar143 string,
  post_evar144 string,
  post_evar145 string,
  post_evar146 string,
  post_evar147 string,
  post_evar148 string,
  post_evar149 string,
  post_evar15 string,
  post_evar150 string,
  post_evar151 string,
  post_evar152 string,
  post_evar153 string,
  post_evar154 string,
  post_evar155 string,
  post_evar156 string,
  post_evar157 string,
  post_evar158 string,
  post_evar159 string,
  post_evar16 string,
  post_evar160 string,
  post_evar161 string,
  post_evar162 string,
  post_evar163 string,
  post_evar164 string,
  post_evar165 string,
  post_evar166 string,
  post_evar167 string,
  post_evar168 string,
  post_evar169 string,
  post_evar17 string,
  post_evar170 string,
  post_evar171 string,
  post_evar172 string,
  post_evar173 string,
  post_evar174 string,
  post_evar175 string,
  post_evar176 string,
  post_evar177 string,
  post_evar178 string,
  post_evar179 string,
  post_evar18 string,
  post_evar180 string,
  post_evar181 string,
  post_evar182 string,
  post_evar183 string,
  post_evar184 string,
  post_evar185 string,
  post_evar186 string,
  post_evar187 string,
  post_evar188 string,
  post_evar189 string,
  post_evar19 string,
  post_evar190 string,
  post_evar191 string,
  post_evar192 string,
  post_evar193 string,
  post_evar194 string,
  post_evar195 string,
  post_evar196 string,
  post_evar197 string,
  post_evar198 string,
  post_evar199 string,
  post_evar2 string,
  post_evar20 string,
  post_evar200 string,
  post_evar201 string,
  post_evar202 string,
  post_evar203 string,
  post_evar204 string,
  post_evar205 string,
  post_evar206 string,
  post_evar207 string,
  post_evar208 string,
  post_evar209 string,
  post_evar21 string,
  post_evar210 string,
  post_evar211 string,
  post_evar212 string,
  post_evar213 string,
  post_evar214 string,
  post_evar215 string,
  post_evar216 string,
  post_evar217 string,
  post_evar218 string,
  post_evar219 string,
  post_evar22 string,
  post_evar220 string,
  post_evar221 string,
  post_evar222 string,
  post_evar223 string,
  post_evar224 string,
  post_evar225 string,
  post_evar226 string,
  post_evar227 string,
  post_evar228 string,
  post_evar229 string,
  post_evar23 string,
  post_evar230 string,
  post_evar231 string,
  post_evar232 string,
  post_evar233 string,
  post_evar234 string,
  post_evar235 string,
  post_evar236 string,
  post_evar237 string,
  post_evar238 string,
  post_evar239 string,
  post_evar24 string,
  post_evar240 string,
  post_evar241 string,
  post_evar242 string,
  post_evar243 string,
  post_evar244 string,
  post_evar245 string,
  post_evar246 string,
  post_evar247 string,
  post_evar248 string,
  post_evar249 string,
  post_evar25 string,
  post_evar250 string,
  post_evar26 string,
  post_evar27 string,
  post_evar28 string,
  post_evar29 string,
  post_evar3 string,
  post_evar30 string,
  post_evar31 string,
  post_evar32 string,
  post_evar33 string,
  post_evar34 string,
  post_evar35 string,
  post_evar36 string,
  post_evar37 string,
  post_evar38 string,
  post_evar39 string,
  post_evar4 string,
  post_evar40 string,
  post_evar41 string,
  post_evar42 string,
  post_evar43 string,
  post_evar44 string,
  post_evar45 string,
  post_evar46 string,
  post_evar47 string,
  post_evar48 string,
  post_evar49 string,
  post_evar5 string,
  post_evar50 string,
  post_evar51 string,
  post_evar52 string,
  post_evar53 string,
  post_evar54 string,
  post_evar55 string,
  post_evar56 string,
  post_evar57 string,
  post_evar58 string,
  post_evar59 string,
  post_evar6 string,
  post_evar60 string,
  post_evar61 string,
  post_evar62 string,
  post_evar63 string,
  post_evar64 string,
  post_evar65 string,
  post_evar66 string,
  post_evar67 string,
  post_evar68 string,
  post_evar69 string,
  post_evar7 string,
  post_evar70 string,
  post_evar71 string,
  post_evar72 string,
  post_evar73 string,
  post_evar74 string,
  post_evar75 string,
  post_evar76 string,
  post_evar77 string,
  post_evar78 string,
  post_evar79 string,
  post_evar8 string,
  post_evar80 string,
  post_evar81 string,
  post_evar82 string,
  post_evar83 string,
  post_evar84 string,
  post_evar85 string,
  post_evar86 string,
  post_evar87 string,
  post_evar88 string,
  post_evar89 string,
  post_evar9 string,
  post_evar90 string,
  post_evar91 string,
  post_evar92 string,
  post_evar93 string,
  post_evar94 string,
  post_evar95 string,
  post_evar96 string,
  post_evar97 string,
  post_evar98 string,
  post_evar99 string,
  post_event_list string,
  post_hier1 string,
  post_hier2 string,
  post_hier3 string,
  post_hier4 string,
  post_hier5 string,
  post_java_enabled string,
  post_keywords string,
  post_mc_audiences string,
  post_mobileaction string,
  post_mobileappid string,
  post_mobilecampaigncontent string,
  post_mobilecampaignmedium string,
  post_mobilecampaignname string,
  post_mobilecampaignsource string,
  post_mobilecampaignterm string,
  post_mobiledayofweek string,
  post_mobiledayssincefirstuse string,
  post_mobiledayssincelastuse string,
  post_mobiledevice string,
  post_mobilehourofday string,
  post_mobileinstalldate string,
  post_mobilelaunchnumber string,
  post_mobileltv string,
  post_mobilemessageid string,
  post_mobilemessageonline string,
  post_mobileosversion string,
  post_mobilepushoptin string,
  post_mobilepushpayloadid string,
  post_mobileresolution string,
  post_mvvar1 string,
  post_mvvar2 string,
  post_mvvar3 string,
  post_page_event int,
  post_page_event_var1 string,
  post_page_event_var2 string,
  post_page_event_var3 string,
  post_page_type string,
  post_page_url string,
  post_pagename string,
  post_pagename_no_url string,
  post_partner_plugins string,
  post_persistent_cookie string,
  post_pointofinterest string,
  post_pointofinterestdistance string,
  post_product_list string,
  post_prop1 string,
  post_prop10 string,
  post_prop11 string,
  post_prop12 string,
  post_prop13 string,
  post_prop14 string,
  post_prop15 string,
  post_prop16 string,
  post_prop17 string,
  post_prop18 string,
  post_prop19 string,
  post_prop2 string,
  post_prop20 string,
  post_prop21 string,
  post_prop22 string,
  post_prop23 string,
  post_prop24 string,
  post_prop25 string,
  post_prop26 string,
  post_prop27 string,
  post_prop28 string,
  post_prop29 string,
  post_prop3 string,
  post_prop30 string,
  post_prop31 string,
  post_prop32 string,
  post_prop33 string,
  post_prop34 string,
  post_prop35 string,
  post_prop36 string,
  post_prop37 string,
  post_prop38 string,
  post_prop39 string,
  post_prop4 string,
  post_prop40 string,
  post_prop41 string,
  post_prop42 string,
  post_prop43 string,
  post_prop44 string,
  post_prop45 string,
  post_prop46 string,
  post_prop47 string,
  post_prop48 string,
  post_prop49 string,
  post_prop5 string,
  post_prop50 string,
  post_prop51 string,
  post_prop52 string,
  post_prop53 string,
  post_prop54 string,
  post_prop55 string,
  post_prop56 string,
  post_prop57 string,
  post_prop58 string,
  post_prop59 string,
  post_prop6 string,
  post_prop60 string,
  post_prop61 string,
  post_prop62 string,
  post_prop63 string,
  post_prop64 string,
  post_prop65 string,
  post_prop66 string,
  post_prop67 string,
  post_prop68 string,
  post_prop69 string,
  post_prop7 string,
  post_prop70 string,
  post_prop71 string,
  post_prop72 string,
  post_prop73 string,
  post_prop74 string,
  post_prop75 string,
  post_prop8 string,
  post_prop9 string,
  post_purchaseid string,
  post_referrer string,
  post_s_kwcid string,
  post_search_engine int,
  post_socialaccountandappids string,
  post_socialassettrackingcode string,
  post_socialauthor string,
  post_socialaveragesentiment string,
  post_socialaveragesentiment_deprecated string,
  post_socialcontentprovider string,
  post_socialfbstories string,
  post_socialfbstorytellers string,
  post_socialinteractioncount string,
  post_socialinteractiontype string,
  post_sociallanguage string,
  post_sociallatlong string,
  post_sociallikeadds string,
  post_sociallink string,
  post_sociallink_deprecated string,
  post_socialmentions string,
  post_socialowneddefinitioninsighttype string,
  post_socialowneddefinitioninsightvalue string,
  post_socialowneddefinitionmetric string,
  post_socialowneddefinitionpropertyvspost string,
  post_socialownedpostids string,
  post_socialownedpropertyid string,
  post_socialownedpropertyname string,
  post_socialownedpropertypropertyvsapp string,
  post_socialpageviews string,
  post_socialpostviews string,
  post_socialproperty string,
  post_socialproperty_deprecated string,
  post_socialpubcomments string,
  post_socialpubposts string,
  post_socialpubrecommends string,
  post_socialpubsubscribers string,
  post_socialterm string,
  post_socialtermslist string,
  post_socialtermslist_deprecated string,
  post_socialtotalsentiment string,
  post_state string,
  post_survey string,
  post_t_time_info string,
  post_tnt string,
  post_tnt_action string,
  post_transactionid string,
  post_video string,
  post_videoad string,
  post_videoadinpod string,
  post_videoadplayername string,
  post_videoadpod string,
  post_videochannel string,
  post_videochapter string,
  post_videocontenttype string,
  post_videopath string,
  post_videoplayername string,
  post_videoqoebitrateaverageevar string,
  post_videoqoebitratechangecountevar string,
  post_videoqoebuffercountevar string,
  post_videoqoebuffertimeevar string,
  post_videoqoedroppedframecountevar string,
  post_videoqoeerrorcountevar string,
  post_videoqoetimetostartevar string,
  post_videosegment string,
  post_visid_high bigint,
  post_visid_low bigint,
  post_visid_type int,
  post_zip string,
  prev_page int,
  product_list string,
  product_merchandising string,
  prop1 string,
  prop10 string,
  prop11 string,
  prop12 string,
  prop13 string,
  prop14 string,
  prop15 string,
  prop16 string,
  prop17 string,
  prop18 string,
  prop19 string,
  prop2 string,
  prop20 string,
  prop21 string,
  prop22 string,
  prop23 string,
  prop24 string,
  prop25 string,
  prop26 string,
  prop27 string,
  prop28 string,
  prop29 string,
  prop3 string,
  prop30 string,
  prop31 string,
  prop32 string,
  prop33 string,
  prop34 string,
  prop35 string,
  prop36 string,
  prop37 string,
  prop38 string,
  prop39 string,
  prop4 string,
  prop40 string,
  prop41 string,
  prop42 string,
  prop43 string,
  prop44 string,
  prop45 string,
  prop46 string,
  prop47 string,
  prop48 string,
  prop49 string,
  prop5 string,
  prop50 string,
  prop51 string,
  prop52 string,
  prop53 string,
  prop54 string,
  prop55 string,
  prop56 string,
  prop57 string,
  prop58 string,
  prop59 string,
  prop6 string,
  prop60 string,
  prop61 string,
  prop62 string,
  prop63 string,
  prop64 string,
  prop65 string,
  prop66 string,
  prop67 string,
  prop68 string,
  prop69 string,
  prop7 string,
  prop70 string,
  prop71 string,
  prop72 string,
  prop73 string,
  prop74 string,
  prop75 string,
  prop8 string,
  prop9 string,
  purchaseid string,
  quarterly_visitor int,
  ref_domain string,
  ref_type int,
  referrer string,
  resolution int,
  s_kwcid string,
  s_resolution string,
  sampled_hit string,
  search_engine int,
  search_page_num int,
  secondary_hit int,
  service string,
  socialaccountandappids string,
  socialassettrackingcode string,
  socialauthor string,
  socialaveragesentiment string,
  socialaveragesentiment_deprecated string,
  socialcontentprovider string,
  socialfbstories string,
  socialfbstorytellers string,
  socialinteractioncount string,
  socialinteractiontype string,
  sociallanguage string,
  sociallatlong string,
  sociallikeadds string,
  sociallink string,
  sociallink_deprecated string,
  socialmentions string,
  socialowneddefinitioninsighttype string,
  socialowneddefinitioninsightvalue string,
  socialowneddefinitionmetric string,
  socialowneddefinitionpropertyvspost string,
  socialownedpostids string,
  socialownedpropertyid string,
  socialownedpropertyname string,
  socialownedpropertypropertyvsapp string,
  socialpageviews string,
  socialpostviews string,
  socialproperty string,
  socialproperty_deprecated string,
  socialpubcomments string,
  socialpubposts string,
  socialpubrecommends string,
  socialpubsubscribers string,
  socialterm string,
  socialtermslist string,
  socialtermslist_deprecated string,
  socialtotalsentiment string,
  sourceid int,
  state string,
  stats_server string,
  t_time_info string,
  tnt string,
  tnt_action string,
  tnt_post_vista string,
  transactionid string,
  truncated_hit string,
  ua_color string,
  ua_os string,
  ua_pixels string,
  user_agent string,
  user_hash int,
  user_server string,
  userid int,
  username string,
  va_closer_detail int,
  va_closer_id string,
  va_finder_detail string,
  va_finder_id int,
  va_instance_event int,
  va_new_engagement int,
  video string,
  videoad string,
  videoadinpod string,
  videoadplayername string,
  videoadpod string,
  videochannel string,
  videochapter string,
  videocontenttype string,
  videopath string,
  videoplayername string,
  videoqoebitrateaverageevar string,
  videoqoebitratechangecountevar string,
  videoqoebuffercountevar string,
  videoqoebuffertimeevar string,
  videoqoedroppedframecountevar string,
  videoqoeerrorcountevar string,
  videoqoetimetostartevar string,
  videosegment string,
  visid_high bigint,
  visid_low bigint,
  visid_new string,
  visid_timestamp bigint,
  visid_type int,
  visit_keywords string,
  visit_num int,
  visit_page_num int,
  visit_ref_domain string,
  visit_ref_type string,
  visit_referrer string,
  visit_search_engine int,
  visit_start_page_url string,
  visit_start_pagename string,
  visit_start_time_gmt bigint,
  weekly_visitor int,
  yearly_visitor int,
  zip bigint
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/apps/hive/warehouse/${env:TMP_db}.db/nifi_sbnet_twc_raw'
TBLPROPERTIES('serialization.null.format'='')
;