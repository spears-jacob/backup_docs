USE ${env:ENVIRONMENT};

---- ##### SB.NET ##### ----

DROP TABLE IF EXISTS ${env:TMP_db}.sbnet_daily_visits_metrics;
CREATE TABLE ${env:TMP_db}.sbnet_daily_visits_metrics AS
SELECT
  'sbnet_events' AS source_file,
  CASE customer WHEN 'Charter Communications' THEN 'L-CHTR' ELSE customer END AS company,
  visit__account__enc_account_number AS account_id,
  visit__device__enc_uuid AS visitor_id,
  visit__visit_id AS visit_id,
  MAX(visit__visit_start_timestamp) AS visit_start_timestamp,
  (MAX(message__timestamp) - MIN(message__timestamp)) AS visit_duration,
  MAX(visit__previous_visit_id) AS previous_visit_id,
  sum(if(message__name = 'Sign In Submit',1,0)) AS login_attempts,
  sum(if(visit__application_details__referrer_link = 'Sign In',1,0)) AS login_successes,
  sum(if(message__category = 'Page View',1,0)) AS total_page_views,
  sum(if(message__category = 'Page View',1,0)) - sum(if(message__category = 'Page View'
    AND state__view__current_page__page_name = 'Login' AND state__view__current_page__section = 'Login' ,1,0)) AS page_views_excluding_login,
  MAX(visit__location__country_name) AS country,
  MAX(visit__location__timezone) AS timezone,
  MAX(visit__location__state) AS state,
  MAX(visit__location__enc_city) AS city,
  MAX(visit__location__enc_zip_code) AS zip,
  MAX(visit__device__browser__name) AS browser,
  MAX(visit__device__operating_system) AS os,
  MAX(visit__screen_resolution) AS resolution,
  MAX(visit__connection__connection_type) AS connection_type,
  MAX(visit__isp__isp) AS isp,
  MAX(visit__isp__enc_ip_address) AS ip
FROM
  sbnet_events
WHERE (partition_date_hour_utc >= concat('${env:YESTERDAY_DEN}', '_', '${env:TZ_OFFSET_DEN}') 
      and
      partition_date_hour_utc < concat('${env:TODAY_DEN}', '_', '${env:TZ_OFFSET_DEN}'))
GROUP BY 
  customer,
  visit__account__enc_account_number,
  visit__device__enc_uuid,
  visit__visit_id
;

INSERT INTO TABLE sbnet_combined_visits_agg PARTITION(partition_date_denver='${env:YESTERDAY_DEN}')
SELECT
  company,
  account_id,
  visitor_id,
  visit_id,
  visit_start_timestamp,
  visit_duration,  
  previous_visit_id,
  lkp_tbl.metric AS metric,
  CASE lkp_tbl.metric
    WHEN 'Login Attempts' THEN src_tbl.login_attempts
    WHEN 'Login Successes' THEN src_tbl.login_successes
    WHEN 'Total Page Views' THEN src_tbl.total_page_views
    WHEN 'Page Views Excluding Login' THEN src_tbl.page_views_excluding_login
  END AS value,
  country,
  timezone,
  state,
  city,
  zip,
  browser,
  os,
  resolution,
  connection_type,
  isp,
  ip
FROM 
  ${env:TMP_db}.sbnet_daily_visits_metrics src_tbl
  LEFT OUTER JOIN
  ${env:LKP_db}.sbnet_daily_agg_metrics lkp_tbl
    ON (src_tbl.source_file = lkp_tbl.source)
;

---- ##### TWC My Account ##### ----

DROP TABLE IF EXISTS ${env:TMP_db}.sbnet_twc_daily_visits_metrics;
CREATE TABLE ${env:TMP_db}.sbnet_twc_daily_visits_metrics AS
SELECT
  'twcmyacct_events' AS source_file,
  CASE customer WHEN 'Charter Communications' THEN 'L-CHTR' ELSE customer END AS company,
  visit__account__enc_account_number AS account_id,
  visit__device__enc_uuid AS visitor_id,
  visit__visit_id AS visit_id,
  MAX(visit__visit_start_timestamp) AS visit_start_timestamp,
  (MAX(message__timestamp) - MIN(message__timestamp)) AS visit_duration,
  MAX(visit__previous_visit_id) AS previous_visit_id,
  sum(if(array_contains(message__name,'My Account Login'),1,0)) + sum(if(state__view__current_page__page_id like '%loginError%',1,0)) AS login_attempts,
  sum(if(array_contains(message__name,'My Account Login'),1,0)) AS login_successes,
  sum(if(message__category = 'Page View',1,0)) AS total_page_views,
  sum(if(message__category = 'Page View',1,0)) - sum(if(message__category = 'Page View' 
    AND state__view__current_page__page_name = 'my account > login',1,0)) AS page_views_excluding_login,
  MAX(visit__location__country_name) AS country,
  MAX(visit__location__timezone) AS timezone,
  MAX(visit__location__state) AS state,
  MAX(visit__location__enc_city) AS city,
  MAX(visit__location__enc_zip_code) AS zip,
  MAX(visit__device__browser__name) AS browser,
  MAX(visit__device__operating_system) AS os,
  MAX(visit__screen_resolution) AS resolution,
  MAX(visit__connection__connection_type) AS connection_type,
  MAX(visit__isp__isp) AS isp,
  MAX(visit__isp__enc_ip_address) AS ip
FROM
  twcmyacct_events
WHERE (partition_date_hour_utc >= concat('${env:YESTERDAY_DEN}', '_', '${env:TZ_OFFSET_DEN}') 
      and
      partition_date_hour_utc < concat('${env:TODAY_DEN}', '_', '${env:TZ_OFFSET_DEN}'))
GROUP BY 
  customer,
  visit__account__enc_account_number,
  visit__device__enc_uuid,
  visit__visit_id
;

INSERT INTO TABLE sbnet_combined_visits_agg PARTITION(partition_date_denver='${env:YESTERDAY_DEN}')
SELECT
  company,
  account_id,
  visitor_id,
  visit_id,
  visit_start_timestamp,
  visit_duration,  
  previous_visit_id,
  lkp_tbl.metric AS metric,
  CASE lkp_tbl.metric
    WHEN 'Login Attempts' THEN src_tbl.login_attempts
    WHEN 'Login Successes' THEN src_tbl.login_successes
    WHEN 'Total Page Views' THEN src_tbl.total_page_views
    WHEN 'Page Views Excluding Login' THEN src_tbl.page_views_excluding_login
  END AS value,
  country,
  timezone,
  state,
  city,
  zip,
  browser,
  os,
  resolution,
  connection_type,
  isp,
  ip
FROM 
  ${env:TMP_db}.sbnet_twc_daily_visits_metrics src_tbl
  LEFT OUTER JOIN
  ${env:LKP_db}.sbnet_daily_agg_metrics lkp_tbl
    ON (src_tbl.source_file = lkp_tbl.source)
;

---- ##### BHN My Services ##### ----

DROP TABLE IF EXISTS ${env:TMP_db}.sbnet_bhn_daily_visits_metrics;
CREATE TABLE ${env:TMP_db}.sbnet_bhn_daily_visits_metrics AS
SELECT
  'bhnmyservices_events' AS source_file,
  CASE customer WHEN 'Charter Communications' THEN 'L-CHTR' ELSE customer END AS company,
  visit__account__enc_account_number AS account_id,
  visit__device__enc_uuid AS visitor_id,
  visit__visit_id AS visit_id,
  MAX(visit__visit_start_timestamp) AS visit_start_timestamp,
  (MAX(message__timestamp) - MIN(message__timestamp)) AS visit_duration,
  MAX(visit__previous_visit_id) AS previous_visit_id,
  CAST(null AS INT) AS login_attempts,
  CAST(null AS INT) AS login_successes,
  sum(if(message__category = 'Page View',1,0)) AS total_page_views,
  sum(if(message__category = 'Page View',1,0)) - sum(if(state__view__current_page__page_id like '%login%',1,0)) AS page_views_excluding_login,
  MAX(visit__location__country_name) AS country,
  MAX(visit__location__timezone) AS timezone,
  MAX(visit__location__state) AS state,
  MAX(visit__location__enc_city) AS city,
  MAX(visit__location__enc_zip_code) AS zip,
  MAX(visit__device__browser__name) AS browser,
  MAX(visit__device__operating_system) AS os,
  MAX(visit__screen_resolution) AS resolution,
  MAX(visit__connection__connection_type) AS connection_type,
  MAX(visit__isp__isp) AS isp,
  MAX(visit__isp__enc_ip_address) AS ip
FROM
  bhnmyservices_events
WHERE 
  ( partition_date_hour_utc >= concat('${env:YESTERDAY_DEN}', '_', '${env:TZ_OFFSET_DEN}') 
    AND partition_date_hour_utc < concat('${env:TODAY_DEN}', '_', '${env:TZ_OFFSET_DEN}'))
  AND ( 
    state__view__current_page__page_type = 'small-medium'
    AND state__view__current_page__page_id NOT IN ('bhnbusiness', 
          'http://businesssolutions.brighthouse.com/home.html',
          'http://businesssolutions.brighthouse.com/content/mobile/business/home.touch.html')
      )
GROUP BY 
  customer,
  visit__account__enc_account_number,
  visit__device__enc_uuid,
  visit__visit_id
;

INSERT INTO TABLE sbnet_combined_visits_agg PARTITION(partition_date_denver='${env:YESTERDAY_DEN}')
SELECT
  company,
  account_id,
  visitor_id,
  visit_id,
  visit_start_timestamp,
  visit_duration,  
  previous_visit_id,
  lkp_tbl.metric AS metric,
  CASE lkp_tbl.metric
    WHEN 'Login Attempts' THEN src_tbl.login_attempts
    WHEN 'Login Successes' THEN src_tbl.login_successes
    WHEN 'Total Page Views' THEN src_tbl.total_page_views
    WHEN 'Page Views Excluding Login' THEN src_tbl.page_views_excluding_login
  END AS value,
  country,
  timezone,
  state,
  city,
  zip,
  browser,
  os,
  resolution,
  connection_type,
  isp,
  ip
FROM 
  ${env:TMP_db}.sbnet_bhn_daily_visits_metrics src_tbl
  LEFT OUTER JOIN
  ${env:LKP_db}.sbnet_daily_agg_metrics lkp_tbl
    ON (src_tbl.source_file = lkp_tbl.source)
;