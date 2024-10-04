USE ${env:NIFI_db};

CREATE TABLE IF NOT EXISTS asp_bhn_residential_event
(
  event_id int,
  detail string
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS asp_bhn_residential_browser
(
  browser_id string,
  detail string
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS asp_bhn_residential_country
(
  country_id int,
  detail string
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS asp_bhn_residential_connection_type
(
  connection_type_id int,
  detail string
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS asp_bhn_residential_javascript
(
  javascript_id int,
  detail string
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS asp_bhn_residential_language
(
  language_id int,
  detail string
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS asp_bhn_residential_os
(
  os_id int,
  detail string
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS asp_bhn_residential_resolution
(
  resolution_id int,
  detail string
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE TABLE IF NOT EXISTS asp_bhn_residential_search_engine
(
  search_engine_id int,
  detail string
)
PARTITIONED BY (partition_date string, partition_hour string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
;

CREATE EXTERNAL TABLE IF NOT EXISTS asp_bhn_residential_raw
(
  accept_language String,
  aemassetid String,
  aemassetsource String,
  aemclickedassetid String,
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
  evar101 String,
  evar102 String,
  evar103 String,
  evar104 String,
  evar105 String,
  evar106 String,
  evar107 String,
  evar108 String,
  evar109 String,
  evar110 String,
  evar111 String,
  evar112 String,
  evar113 String,
  evar114 String,
  evar115 String,
  evar116 String,
  evar117 String,
  evar118 String,
  evar119 String,
  evar120 String,
  evar121 String,
  evar122 String,
  evar123 String,
  evar124 String,
  evar125 String,
  evar126 String,
  evar127 String,
  evar128 String,
  evar129 String,
  evar130 String,
  evar131 String,
  evar132 String,
  evar133 String,
  evar134 String,
  evar135 String,
  evar136 String,
  evar137 String,
  evar138 String,
  evar139 String,
  evar140 String,
  evar141 String,
  evar142 String,
  evar143 String,
  evar144 String,
  evar145 String,
  evar146 String,
  evar147 String,
  evar148 String,
  evar149 String,
  evar150 String,
  evar151 String,
  evar152 String,
  evar153 String,
  evar154 String,
  evar155 String,
  evar156 String,
  evar157 String,
  evar158 String,
  evar159 String,
  evar160 String,
  evar161 String,
  evar162 String,
  evar163 String,
  evar164 String,
  evar165 String,
  evar166 String,
  evar167 String,
  evar168 String,
  evar169 String,
  evar170 String,
  evar171 String,
  evar172 String,
  evar173 String,
  evar174 String,
  evar175 String,
  evar176 String,
  evar177 String,
  evar178 String,
  evar179 String,
  evar180 String,
  evar181 String,
  evar182 String,
  evar183 String,
  evar184 String,
  evar185 String,
  evar186 String,
  evar187 String,
  evar188 String,
  evar189 String,
  evar190 String,
  evar191 String,
  evar192 String,
  evar193 String,
  evar194 String,
  evar195 String,
  evar196 String,
  evar197 String,
  evar198 String,
  evar199 String,
  evar200 String,
  evar201 String,
  evar202 String,
  evar203 String,
  evar204 String,
  evar205 String,
  evar206 String,
  evar207 String,
  evar208 String,
  evar209 String,
  evar210 String,
  evar211 String,
  evar212 String,
  evar213 String,
  evar214 String,
  evar215 String,
  evar216 String,
  evar217 String,
  evar218 String,
  evar219 String,
  evar220 String,
  evar221 String,
  evar222 String,
  evar223 String,
  evar224 String,
  evar225 String,
  evar226 String,
  evar227 String,
  evar228 String,
  evar229 String,
  evar230 String,
  evar231 String,
  evar232 String,
  evar233 String,
  evar234 String,
  evar235 String,
  evar236 String,
  evar237 String,
  evar238 String,
  evar239 String,
  evar240 String,
  evar241 String,
  evar242 String,
  evar243 String,
  evar244 String,
  evar245 String,
  evar246 String,
  evar247 String,
  evar248 String,
  evar249 String,
  evar250 String,
  event_list String,
  exclude_hit Int,
  first_hit_page_url String,
  first_hit_pagename String,
  first_hit_ref_domain String,
  first_hit_ref_type String,
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
  post_evar101 String,
  post_evar102 String,
  post_evar103 String,
  post_evar104 String,
  post_evar105 String,
  post_evar106 String,
  post_evar107 String,
  post_evar108 String,
  post_evar109 String,
  post_evar110 String,
  post_evar111 String,
  post_evar112 String,
  post_evar113 String,
  post_evar114 String,
  post_evar115 String,
  post_evar116 String,
  post_evar117 String,
  post_evar118 String,
  post_evar119 String,
  post_evar120 String,
  post_evar121 String,
  post_evar122 String,
  post_evar123 String,
  post_evar124 String,
  post_evar125 String,
  post_evar126 String,
  post_evar127 String,
  post_evar128 String,
  post_evar129 String,
  post_evar130 String,
  post_evar131 String,
  post_evar132 String,
  post_evar133 String,
  post_evar134 String,
  post_evar135 String,
  post_evar136 String,
  post_evar137 String,
  post_evar138 String,
  post_evar139 String,
  post_evar140 String,
  post_evar141 String,
  post_evar142 String,
  post_evar143 String,
  post_evar144 String,
  post_evar145 String,
  post_evar146 String,
  post_evar147 String,
  post_evar148 String,
  post_evar149 String,
  post_evar150 String,
  post_evar151 String,
  post_evar152 String,
  post_evar153 String,
  post_evar154 String,
  post_evar155 String,
  post_evar156 String,
  post_evar157 String,
  post_evar158 String,
  post_evar159 String,
  post_evar160 String,
  post_evar161 String,
  post_evar162 String,
  post_evar163 String,
  post_evar164 String,
  post_evar165 String,
  post_evar166 String,
  post_evar167 String,
  post_evar168 String,
  post_evar169 String,
  post_evar170 String,
  post_evar171 String,
  post_evar172 String,
  post_evar173 String,
  post_evar174 String,
  post_evar175 String,
  post_evar176 String,
  post_evar177 String,
  post_evar178 String,
  post_evar179 String,
  post_evar180 String,
  post_evar181 String,
  post_evar182 String,
  post_evar183 String,
  post_evar184 String,
  post_evar185 String,
  post_evar186 String,
  post_evar187 String,
  post_evar188 String,
  post_evar189 String,
  post_evar190 String,
  post_evar191 String,
  post_evar192 String,
  post_evar193 String,
  post_evar194 String,
  post_evar195 String,
  post_evar196 String,
  post_evar197 String,
  post_evar198 String,
  post_evar199 String,
  post_evar200 String,
  post_evar201 String,
  post_evar202 String,
  post_evar203 String,
  post_evar204 String,
  post_evar205 String,
  post_evar206 String,
  post_evar207 String,
  post_evar208 String,
  post_evar209 String,
  post_evar210 String,
  post_evar211 String,
  post_evar212 String,
  post_evar213 String,
  post_evar214 String,
  post_evar215 String,
  post_evar216 String,
  post_evar217 String,
  post_evar218 String,
  post_evar219 String,
  post_evar220 String,
  post_evar221 String,
  post_evar222 String,
  post_evar223 String,
  post_evar224 String,
  post_evar225 String,
  post_evar226 String,
  post_evar227 String,
  post_evar228 String,
  post_evar229 String,
  post_evar230 String,
  post_evar231 String,
  post_evar232 String,
  post_evar233 String,
  post_evar234 String,
  post_evar235 String,
  post_evar236 String,
  post_evar237 String,
  post_evar238 String,
  post_evar239 String,
  post_evar240 String,
  post_evar241 String,
  post_evar242 String,
  post_evar243 String,
  post_evar244 String,
  post_evar245 String,
  post_evar246 String,
  post_evar247 String,
  post_evar248 String,
  post_evar249 String,
  post_evar250 String,
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
  post_socialaveragesentiment String,
  post_socialaveragesentiment_deprecated String,
  post_socialcontentprovider String,
  post_socialfbstories String,
  post_socialfbstorytellers String,
  post_socialinteractioncount String,
  post_socialinteractiontype String,
  post_sociallanguage String,
  post_sociallatlong String,
  post_sociallikeadds String,
  post_sociallink String,
  post_sociallink_deprecated String,
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
  post_socialproperty String,
  post_socialproperty_deprecated String,
  post_socialpubcomments String,
  post_socialpubposts String,
  post_socialpubrecommends String,
  post_socialpubsubscribers String,
  post_socialterm String,
  post_socialtermslist String,
  post_socialtermslist_deprecated String,
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
  post_videoadlength String,
  post_videoadname String,
  post_videoadplayername String,
  post_videoadpod String,
  post_videochannel String,
  post_videochapter String,
  post_videocontenttype String,
  post_videolength String,
  post_videoname String,
  post_videopath String,
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
  socialaveragesentiment String,
  socialaveragesentiment_deprecated String,
  socialcontentprovider String,
  socialfbstories String,
  socialfbstorytellers String,
  socialinteractioncount String,
  socialinteractiontype String,
  sociallanguage String,
  sociallatlong String,
  sociallikeadds String,
  sociallink String,
  sociallink_deprecated String,
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
  socialproperty String,
  socialproperty_deprecated String,
  socialpubcomments String,
  socialpubposts String,
  socialpubrecommends String,
  socialpubsubscribers String,
  socialterm String,
  socialtermslist String,
  socialtermslist_deprecated String,
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
  videoadlength String,
  videoadname String,
  videoadplayername String,
  videoadpod String,
  videochannel String,
  videochapter String,
  videocontenttype String,
  videolength String,
  videoname String,
  videopath String,
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
  visit_ref_domain String,
  visit_ref_type String,
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
LOCATION '/apps/hive/warehouse/${env:NIFI_db}.db/asp_bhn_residential_raw'
TBLPROPERTIES('serialization.null.format'='');
