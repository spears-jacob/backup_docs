USE ${env:ENVIRONMENT};

SELECT '

 ****-- Begin population of quantum data --****

';
----------------------------
-- SS REFERRING DOMAIN DAILY
----------------------------
SELECT '

 ****-- Begin population of referring domain metric --****

';
--Staging

INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

SELECT
      SIZE(COLLECT_SET(visit__device__enc_uuid)) AS value,
      state__view__previous_page__page_name as detail,
      'devices' AS unit,
      'visit__application_details__referrer_link' as metric,
      'asp' AS platform,
      'smb' AS domain,
      coalesce(visit__account__details__mso,'UNKNOWN') as company,
      epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
      'asp_v_venona_staging_portals_smb' as source_table
 FROM  asp_v_venona_staging_portals_smb
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
  AND partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND LOWER(state__view__current_page__app_section) = lower('support')
  AND message__name= 'pageView'
GROUP BY
      epoch_converter(cast(received__timestamp as bigint),'America/Denver')
    , state__view__previous_page__page_name
    ,coalesce(visit__account__details__mso,'UNKNOWN');

-- prod

INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

SELECT
      SIZE(COLLECT_SET(visit__device__enc_uuid)) AS value,
      state__view__previous_page__page_name as detail,
      'devices' AS unit,
      'visit__application_details__referrer_link' as metric,
      'asp' AS platform,
      'smb' AS domain,
      coalesce(visit__account__details__mso,'UNKNOWN') as company,
      epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
      'asp_v_venona_events_portals_smb' as source_table
 FROM  asp_v_venona_events_portals_smb
WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
  AND partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND LOWER(state__view__current_page__app_section) = lower('support')
  AND message__name= 'pageView'
GROUP BY
      epoch_converter(cast(received__timestamp as bigint),'America/Denver')
    , state__view__previous_page__page_name
    ,coalesce(visit__account__details__mso,'UNKNOWN');

SELECT '

  ****-- END population of referring domain metric --****

';
--------------------------------------------------------------------------------

----------------------------
-- AVG_SUPPORT_PAGE_VIEW_PER_VISIT DAILY
----------------------------

SELECT '

 ****-- Begin population of avg support page view per visit metric --****

';

-- staging

INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

select AVG(number_of_support) AS value,
       'avg_support_page_view_per_visit' as detail,
       'instances' AS unit,
       'avg_support_page_view_per_visit' as metric,
       'asp' AS platform,
       'smb' AS domain,
       company,
       date_denver,
       'asp_v_venona_staging_portals_smb' as source_table
FROM
      (SELECT
            concat(visit__account__enc_account_number ,'_', visit__visit_id) as detail,
            SUm(IF( message__name = 'pageView'
                and LOWER(state__view__current_page__app_section) = lower('support'), 1, 0)) AS number_of_support,
            SUM(IF( message__name = 'pageView'
                and LOWER(state__view__current_page__app_section) = LOWER('support')
                AND LOWER(state__view__current_page__page_name) NOT IN (LOWER('Support Home'), LOWER('SMB - Support Home')),
                1, 0)) AS number_of_support_article,
            epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
             coalesce(visit__account__details__mso,'UNKNOWN') as company
       FROM asp_v_venona_staging_portals_smb
       WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
         AND partition_date_hour_utc < '${env:END_DATE_TZ}')
         -- exclude null and pending_login account numbers
         AND visit__account__enc_account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
      GROUP BY
            epoch_converter(cast(received__timestamp as bigint),'America/Denver')
          , concat(visit__account__enc_account_number ,'_', visit__visit_id)
          , coalesce(visit__account__details__mso,'UNKNOWN')) a
WHERE number_of_support_article > 0
group by date_denver,company ;

-- prod

INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

select AVG(number_of_support) AS value,
       'avg_support_page_view_per_visit' as detail,
       'instances' AS unit,
       'avg_support_page_view_per_visit' as metric,
       'asp' AS platform,
       'smb' AS domain,
       company,
       date_denver,
       'asp_v_venona_events_portals_smb' as source_table
FROM
      (SELECT
            concat(visit__account__enc_account_number ,'_', visit__visit_id) as detail,
            SUm(IF( message__name = 'pageView'
                and LOWER(state__view__current_page__app_section) = lower('support'), 1, 0)) AS number_of_support,
            SUM(IF( message__name = 'pageView'
                and LOWER(state__view__current_page__app_section) = lower('support')
                AND LOWER(state__view__current_page__page_name) NOT IN (lower('Support Home'), lower('SMB - Support Home')),
                1, 0)) AS number_of_support_article,
            epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
             coalesce(visit__account__details__mso,'UNKNOWN') as company
       FROM  asp_v_venona_events_portals_smb
       WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
         AND partition_date_hour_utc < '${env:END_DATE_TZ}')
         -- exclude null and pending_login account numbers
         AND visit__account__enc_account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
      GROUP BY
            epoch_converter(cast(received__timestamp as bigint),'America/Denver')
          , concat(visit__account__enc_account_number ,'_', visit__visit_id)
          , coalesce(visit__account__details__mso,'UNKNOWN')) a
WHERE number_of_support_article > 0
group by date_denver,company;

SELECT '

 ****-- END population of avg support page view per visit metric --****

';
--------------------------------------------------------------------------------

----------------------------
-- SS_REFERRING_SITE_SECTION DAILY
----------------------------

SELECT '

 ****-- Begin population of ss referring site section metric --****

';

-- staging
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

SELECT
      SIZE(COLLECT_SET(visit__device__enc_uuid)) AS value,
      state__view__previous_page__app_section as detail,
      'devices' AS unit,
      'ss_referring_site_section' as metric,
      'asp' AS platform,
      'smb' AS domain,
      coalesce(visit__account__details__mso,'UNKNOWN') as company,
      epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
      'asp_v_venona_staging_portals_smb' as source_table
 FROM  asp_v_venona_staging_portals_smb
 WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND LOWER(state__view__current_page__app_section) = lower('support')
  AND message__name = 'pageView'
GROUP BY
      epoch_converter(cast(received__timestamp as bigint),'America/Denver')
    , state__view__previous_page__app_section
    ,coalesce(visit__account__details__mso,'UNKNOWN')
;;

-- prod
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

SELECT
      SIZE(COLLECT_SET(visit__device__enc_uuid)) AS value,
      state__view__previous_page__app_section as detail,
      'devices' AS unit,
      'ss_referring_site_section' as metric,
      'asp' AS platform,
      'smb' AS domain,
      coalesce(visit__account__details__mso,'UNKNOWN') as company,
      epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
      'asp_v_venona_events_portals_smb' as source_table
 FROM  asp_v_venona_events_portals_smb
 WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND LOWER(state__view__current_page__app_section) = LOWER('support')
  AND message__name = 'pageView'
GROUP BY
      epoch_converter(cast(received__timestamp as bigint),'America/Denver')
    , state__view__previous_page__app_section
    ,coalesce(visit__account__details__mso,'UNKNOWN');

SELECT '

 ****-- END population of ss referring site section metric --****

';
--------------------------------------------------------------------------------

----------------------------
-- SS_SUPPORT_ARTICLES_AUTH DAILY
----------------------------

SELECT '

 ****-- Begin population of ss support articles auth metric --****

';

-- staging
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)
SELECT
      SIZE(COLLECT_SET(visit__device__enc_uuid)) AS value,
      state__view__current_page__page_name as detail,
      'devices' AS unit,
      'ss_support_articles_auth' as metric,
      'asp' AS platform,
      'smb' AS domain,
      coalesce(visit__account__details__mso,'UNKNOWN') as company,
      epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
      'asp_v_venona_staging_portals_smb' as source_table
 FROM asp_v_venona_staging_portals_smb
 WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND LOWER(state__view__current_page__app_section) = LOWER('support')
  AND message__name = 'pageView'
  -- exclude null and pending_login account numbers
  AND visit__account__enc_account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
GROUP BY
      epoch_converter(cast(received__timestamp as bigint),'America/Denver')
    , state__view__current_page__page_name
    ,coalesce(visit__account__details__mso,'UNKNOWN')
;

-- prod
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)
SELECT
      SIZE(COLLECT_SET(visit__device__enc_uuid)) AS value,
      state__view__current_page__page_name as detail,
      'devices' AS unit,
      'ss_support_articles_auth' as metric,
      'asp' AS platform,
      'smb' AS domain,
      coalesce(visit__account__details__mso,'UNKNOWN') as company,
      epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
      'asp_v_venona_events_portals_smb' as source_table
 FROM  asp_v_venona_events_portals_smb
 WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND LOWER(state__view__current_page__app_section) = LOWER('support')
  AND message__name = 'pageView'
  -- exclude null and pending_login account numbers
  AND visit__account__enc_account_number NOT IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
GROUP BY
      epoch_converter(cast(received__timestamp as bigint),'America/Denver')
    , state__view__current_page__page_name
    ,coalesce(visit__account__details__mso,'UNKNOWN')
;

SELECT '

 ****-- END population of ss support articles auth metric --****

';

--------------------------------------------------------------------------------

----------------------------
-- SS_SUPPORT_ARTICLES_UNAUTH DAILY
----------------------------
SELECT '

 ****-- Begin population of ss support articles unauth metric --****

';

--  staging
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

SELECT
      SIZE(COLLECT_SET(visit__device__enc_uuid)) AS value,
      state__view__current_page__page_name as detail,
      'devices' AS unit,
      'ss_support_articles_unauth' as metric,
      'asp' AS platform,
      'smb' AS domain,
      coalesce(visit__account__details__mso,'UNKNOWN') as company,
      epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
      'asp_v_venona_staging_portals_smb' as source_table
 FROM  asp_v_venona_staging_portals_smb
 WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND LOWER(state__view__current_page__app_section) = LOWER('support')
  AND message__name = 'pageView'
  -- include null and pending_login account numbers
  AND visit__account__enc_account_number IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
GROUP BY
      epoch_converter(cast(received__timestamp as bigint),'America/Denver')
    , state__view__current_page__page_name
    ,coalesce(visit__account__details__mso,'UNKNOWN')
;

--prod
INSERT OVERWRITE TABLE asp_metrics_detail
PARTITION(metric,platform,domain,company,date_denver,source_table)

SELECT
      SIZE(COLLECT_SET(visit__device__enc_uuid)) AS value,
      state__view__current_page__page_name as detail,
      'devices' AS unit,
      'ss_support_articles_unauth' as metric,
      'asp' AS platform,
      'smb' AS domain,
      coalesce(visit__account__details__mso,'UNKNOWN') as company,
      epoch_converter(cast(received__timestamp as bigint),'America/Denver') as date_denver,
      'asp_v_venona_events_portals_smb' as source_table
 FROM asp_v_venona_events_portals_smb
 WHERE (partition_date_hour_utc >= '${env:START_DATE_TZ}'
   AND partition_date_hour_utc < '${env:END_DATE_TZ}')
  AND LOWER(state__view__current_page__app_section) = LOWER('support')
  AND message__name = 'pageView'
  -- include null and pending_login account numbers
  AND visit__account__enc_account_number IN('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==')
GROUP BY
      epoch_converter(cast(received__timestamp as bigint),'America/Denver')
    , state__view__current_page__page_name
    ,coalesce(visit__account__details__mso,'UNKNOWN')
;

SELECT '

 ****-- END population of ss support articles unauth metric --****

';
--------------------------------------------------------------------------------

SELECT '

 ****-- Begin population of quantum data --****

';
