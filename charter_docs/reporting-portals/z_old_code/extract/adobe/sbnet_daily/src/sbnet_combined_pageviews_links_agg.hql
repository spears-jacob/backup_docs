USE ${env:ENVIRONMENT};

INSERT INTO TABLE sbnet_combined_pageviews_links_agg PARTITION(partition_date_denver='${env:YESTERDAY_DEN}')
SELECT
  CASE customer WHEN 'Charter Communications' THEN 'L-CHTR' ELSE customer END AS company,
  visit__account__enc_account_number AS account_id,
  visit__device__enc_uuid AS visitor_id,
  visit__visit_id AS visit_id,
  visit__visit_start_timestamp AS visit_start_timestamp,
  visit__previous_visit_id AS previous_visit_id,
  message__category AS category,
  message__timestamp AS message_timestamp,
  state__view__current_page__page_id AS page_url,
  CASE
    WHEN visit__user__role LIKE 'Logged In%' Then 'Authenticated'
    WHEN visit__user__role LIKE 'Logged Out%' Then 'Unauthenticated'
    ELSE visit__user__role
  END AS auth,
  'spectrumbusiness.net' AS site_name,
  state__view__current_page__page_name AS page_name,
  state__view__current_page__section AS site_section,
  state__view__current_page__sub_section AS sub_section,
  CASE message__category 
    WHEN 'Page View' THEN ARRAY("")
    WHEN 'Custom Link' THEN ARRAY(state__view__current_page__elements__name)
  END AS link_name,
  CASE message__category 
    WHEN 'Page View' THEN ARRAY("")
    WHEN 'Custom Link' THEN ARRAY(message__name)
  END AS link_details,
  visit__application_details__referrer_link AS referrer_link,
  state__view__previous_page__page_id AS previous_page_url,
  state__view__previous_page__page_name AS previous_page_name,
  state__view__previous_page__section AS previous_site_section,
  state__view__previous_page__sub_section AS previous_sub_section,
  CASE message__category 
    WHEN 'Page View' THEN state__search__text
    WHEN 'Custom Link' THEN null
  END AS search_terms,
  CASE message__category 
    WHEN 'Page View' THEN state__search__results
    WHEN 'Custom Link' THEN ARRAY("")
  END AS search_results
FROM
  sbnet_events
WHERE (partition_date_hour_utc >= concat('${env:YESTERDAY_DEN}', '_', '${env:TZ_OFFSET_DEN}') 
      and
      partition_date_hour_utc < concat('${env:TODAY_DEN}', '_', '${env:TZ_OFFSET_DEN}'))
;

INSERT INTO TABLE sbnet_combined_pageviews_links_agg PARTITION(partition_date_denver='${env:YESTERDAY_DEN}')
SELECT
  CASE customer WHEN 'Charter Communications' THEN 'L-CHTR' ELSE customer END AS company,
  visit__account__enc_account_number AS account_id,
  visit__device__enc_uuid AS visitor_id,
  visit__visit_id AS visit_id,
  visit__visit_start_timestamp AS visit_start_timestamp,
  visit__previous_visit_id AS previous_visit_id,
  message__category AS category,
  message__timestamp AS message_timestamp,
  state__view__current_page__page_id AS page_url,
  CASE
    WHEN visit__account__enc_account_number LIKE '%not set%' THEN 'Unauthenticated'
    ELSE 'Authenticated'
  END AS auth,
  'myaccount.timewarnercable.com' AS site_name,
  CASE message__category 
    WHEN 'Page View' THEN 
      CASE
        WHEN state__view__current_page__page_name LIKE '%hoopla%' THEN substr(state__view__current_page__page_name,23)
        WHEN state__view__current_page__page_name LIKE '%perkzone%' THEN substr(state__view__current_page__page_name,25)
        WHEN state__view__current_page__page_name LIKE '%support%' THEN substr(state__view__current_page__page_name,24)
        WHEN state__view__current_page__page_name LIKE 'my account >%' THEN substr(state__view__current_page__page_name,14)
        ELSE state__view__current_page__page_name
       END
    WHEN 'Custom Link' THEN null
  END AS page_name,
  CASE message__category 
    WHEN 'Page View' THEN 
      CASE
        WHEN state__view__current_page__section = 'billing' THEN 'Billing'
        WHEN state__view__current_page__section = 'error' THEN 'Error'
        WHEN state__view__current_page__section = 'hoopla' THEN 'Hoopla'
        WHEN state__view__current_page__section = 'login' THEN 'Login'
        WHEN state__view__current_page__section = 'outage' THEN 'Outage'
        WHEN state__view__current_page__section = 'overview' THEN 'Overview'
        WHEN state__view__current_page__section = 'perkzone' THEN 'Perkzone'
        WHEN state__view__current_page__section = 'profile' THEN 'Profile'
        WHEN state__view__current_page__section = 'services' THEN 'Services' 
        WHEN state__view__current_page__section = 'support' THEN 'Support'
        WHEN state__view__current_page__section LIKE '%track order%' THEN 'Track Order'
        WHEN state__view__current_page__section = 'users' THEN 'Users'
        ELSE state__view__current_page__section
       END
    WHEN 'Custom Link' THEN 
      CASE
        WHEN state__view__current_page__elements__name LIKE 'my account > billing%' THEN 'Billing'
        WHEN state__view__current_page__elements__name LIKE '%profile%' THEN 'Profile'
        WHEN state__view__current_page__elements__name LIKE '%error%' THEN 'Error'
        WHEN state__view__current_page__elements__name LIKE '%Field:%' THEN 'Field'
        WHEN state__view__current_page__elements__name LIKE 'my account > header%' THEN 'Header'
        WHEN state__view__current_page__elements__name LIKE '%hoopla%' THEN 'Hoopla'
        WHEN state__view__current_page__elements__name LIKE '%login%' THEN 'Login'
        WHEN state__view__current_page__elements__name LIKE '%unauth track order%' THEN 'Unauth Track Order'
        WHEN state__view__current_page__elements__name LIKE '%track order%' THEN 'Track Order'
        WHEN state__view__current_page__elements__name LIKE '%overview%' THEN 'Overview'
        WHEN state__view__current_page__elements__name LIKE '%perkzone%' THEN 'Perkzone'
        WHEN state__view__current_page__elements__name LIKE '%perkhub%' THEN 'Perk Hub'
        WHEN state__view__current_page__elements__name LIKE '%services%' THEN 'Services'
        WHEN state__view__current_page__elements__name LIKE '%support%' THEN 'Support'
        WHEN state__view__current_page__elements__name LIKE 'my account > users%' THEN 'Users'
        ELSE state__view__current_page__elements__name
       END
  END AS site_section,
  CASE message__category 
    WHEN 'Page View' THEN state__view__current_page__sub_section
    WHEN 'Custom Link' THEN null
  END AS sub_section,
  CASE message__category 
    WHEN 'Page View' THEN ARRAY("")
    WHEN 'Custom Link' THEN ARRAY(state__view__current_page__elements__name)
  END AS link_name,
  ARRAY("") AS link_details,
  null AS referrer_link,
  state__view__previous_page__page_id AS previous_page_url,
  state__view__previous_page__page_name AS previous_page_name,
  state__view__previous_page__section AS previous_site_section,
  state__view__previous_page__sub_section AS previous_sub_section,
  null AS search_terms,
  ARRAY("") AS search_results
FROM
  twcmyacct_events
WHERE (partition_date_hour_utc >= concat('${env:YESTERDAY_DEN}', '_', '${env:TZ_OFFSET_DEN}') 
      and
      partition_date_hour_utc < concat('${env:TODAY_DEN}', '_', '${env:TZ_OFFSET_DEN}'))
;

INSERT INTO TABLE sbnet_combined_pageviews_links_agg PARTITION(partition_date_denver='${env:YESTERDAY_DEN}')
SELECT
  CASE customer WHEN 'Charter Communications' THEN 'L-CHTR' ELSE customer END AS company,
  visit__account__enc_account_number AS account_id,
  visit__device__enc_uuid AS visitor_id,
  visit__visit_id AS visit_id,
  visit__visit_start_timestamp AS visit_start_timestamp,
  visit__previous_visit_id AS previous_visit_id,
  message__category AS category,
  message__timestamp AS message_timestamp,
  state__view__current_page__page_id AS page_url,
  CASE
    WHEN visit__account__enc_account_number LIKE '%not set%' THEN 'Unauthenticated'
    ELSE 'Authenticated'
  END AS auth,
  'business.timewarnercable.com' AS site_name,
  state__view__current_page__page_name AS page_name,
  state__view__current_page__section AS site_section,
  state__view__current_page__sub_section AS sub_section,
  CASE message__category 
    WHEN 'Page View' THEN ARRAY("")
    WHEN 'Custom Link' THEN ARRAY(state__view__current_page__elements__name)
  END AS link_name,
  CASE message__category 
    WHEN 'Page View' THEN ARRAY("")
    WHEN 'Custom Link' THEN message__name
  END AS link_details,
  null AS referrer_link,
  null AS previous_page_url,
  state__view__previous_page__page_name AS previous_page_name,
  null AS previous_site_section,
  null AS previous_sub_section,
  CASE
    WHEN state__search__text like '%2520%' then regexp_replace(state__search__text, '%2520', ' ')
    WHEN state__search__text like '%20%' then regexp_replace(state__search__text, '%20', ' ')
    ELSE state__search__text
  END AS search_terms,
  state__search__results AS search_results
FROM
  twcbusglobal_events
WHERE (partition_date_hour_utc >= concat('${env:YESTERDAY_DEN}', '_', '${env:TZ_OFFSET_DEN}') 
      and
      partition_date_hour_utc < concat('${env:TODAY_DEN}', '_', '${env:TZ_OFFSET_DEN}'))
;

INSERT INTO TABLE sbnet_combined_pageviews_links_agg PARTITION(partition_date_denver='${env:YESTERDAY_DEN}')
SELECT
  CASE customer WHEN 'Charter Communications' THEN 'L-CHTR' ELSE customer END AS company,
  visit__account__enc_account_number AS account_id,
  visit__device__enc_uuid AS visitor_id,
  visit__visit_id AS visit_id,
  visit__visit_start_timestamp AS visit_start_timestamp,
  visit__previous_visit_id AS previous_visit_id,
  message__category AS category,
  message__timestamp AS message_timestamp,
  state__view__current_page__page_id AS page_url,
  null AS auth,
   'myservices.brighthouse.com' AS site_name,
  state__view__current_page__page_name AS page_name,
  state__view__current_page__section AS site_section,
  null AS sub_section,
  CASE message__category 
    WHEN 'Page View' THEN ARRAY("")
    WHEN 'Custom Link' THEN message__name
  END AS link_name,
  ARRAY("") AS link_details,
  null AS referrer_link,
  null AS previous_page_url,
  state__view__previous_page__page_name AS previous_page_name,
  null AS previous_site_section,
  null AS previous_sub_section,
  null AS search_terms,
  ARRAY("") AS search_results
FROM
  bhnmyservices_events
WHERE (partition_date_hour_utc >= concat('${env:YESTERDAY_DEN}', '_', '${env:TZ_OFFSET_DEN}') 
      and
      partition_date_hour_utc < concat('${env:TODAY_DEN}', '_', '${env:TZ_OFFSET_DEN}'))
;
