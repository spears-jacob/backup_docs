-- ============
-- == STANDARDIZED NAME LIKE NETFLIX CHECK
-- ============
DROP TABLE IF EXISTS test.SAG_netflix_standardized_name_check;
CREATE TABLE test.SAG_netflix_standardized_name_check AS

SELECT
      venona_events.message__category,
      venona_events.message__name,
      venona_events.state__view__current_page__page_name,
      venona_events.state__view__current_page__page_section__name,
      venona_events.state__view__current_page__page_sub_section__name,
      venona_events.state__view__current_page__elements__standardized_name,
      venona_events.operation__operation_type,
      venona_events.visit__external_apps__app_vendor,
      venona_events.visit__external_apps__app_name,
      venona_events.visit__external_apps__entitled,
      venona_events.visit__application_details__app_version,
      venona_events.message__triggered_by,
      venona_events.state__name,
      venona_events.state__content__identifiers__tms_series_id,
      venona_events.state__content__identifiers__tms_program_id,
      venona_events.state__content__identifiers__tms_guide_id,
      venona_events.visit__application_details__application_name,
      count(*) AS count_of_events
FROM prod.venona_events venona_events
WHERE
          partition_date_utc >= '2017-12-01'
      AND
        venona_events.state__view__current_page__elements__standardized_name = 'netflix'
      AND (visit__application_details__application_name = 'Spectrum Guide'
            OR  visit__application_details__application_type = 'SpecGuide'
          )
GROUP BY
      venona_events.message__category,
      venona_events.message__name,
      venona_events.state__view__current_page__page_name,
      venona_events.state__view__current_page__page_section__name,
      venona_events.state__view__current_page__page_sub_section__name,
      venona_events.state__view__current_page__elements__standardized_name,
      venona_events.operation__operation_type,
      venona_events.visit__external_apps__app_vendor,
      venona_events.visit__external_apps__app_name,
      venona_events.visit__external_apps__entitled,
      venona_events.visit__application_details__app_version,
      venona_events.message__triggered_by,
      venona_events.state__name,
      venona_events.state__content__identifiers__tms_series_id,
      venona_events.state__content__identifiers__tms_program_id,
      venona_events.state__content__identifiers__tms_guide_id,
      venona_events.visit__application_details__application_name
      ;


DROP TABLE IF EXISTS test.sag_venona_sample_check;
CREATE TABLE test.sag_venona_sample_check AS
SELECT
      venona_events.visit__device__uuid,
      venona_events.message__category,
      venona_events.message__name,
      venona_events.state__view__current_page__page_name,
      venona_events.state__view__current_page__page_section__name,
      venona_events.state__view__current_page__page_sub_section__name,
      venona_events.state__view__current_page__elements__standardized_name,
      venona_events.operation__operation_type,
      -- venona_events.visit__external_apps__app_vendor,
      -- venona_events.visit__external_apps__app_name,
      -- venona_events.visit__external_apps__entitled,
      venona_events.visit__application_details__app_version,
      venona_events.message__triggered_by,
      venona_events.state__name,
      venona_events.state__content__identifiers__tms_series_id,
      venona_events.state__content__identifiers__tms_program_id,
      venona_events.state__content__identifiers__tms_guide_id,
      venona_events.visit__application_details__application_name,
      count(*) AS count_of_events
FROM prod.venona_events venona_events
WHERE
          partition_date_utc >= '2018-01-22'
      -- AND state__view__content__identifiers__tms_guide_id = 'SH025232490000'
      -- AND state__content__identifiers__tms_series_id = 'SH025232490000'
      AND state__content__identifiers__tms_guide_id = 'SH025232490000'
      AND visit__application_details__application_name = 'Spectrum Guide'

GROUP BY
      venona_events.visit__device__uuid,
      venona_events.message__category,
      venona_events.message__name,
      venona_events.state__view__current_page__page_name,
      venona_events.state__view__current_page__page_section__name,
      venona_events.state__view__current_page__page_sub_section__name,
      venona_events.state__view__current_page__elements__standardized_name,
      venona_events.operation__operation_type,
      -- venona_events.visit__external_apps__app_vendor,
      -- venona_events.visit__external_apps__app_name,
      -- venona_events.visit__external_apps__entitled,
      venona_events.visit__application_details__app_version,
      venona_events.message__triggered_by,
      venona_events.state__name,
      venona_events.state__content__identifiers__tms_series_id,
      venona_events.state__content__identifiers__tms_program_id,
      venona_events.state__content__identifiers__tms_guide_id,
      venona_events.visit__application_details__application_name
      ;


SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

DROP TABLE IF EXISTS test.sag_venona_sample_by_account;
CREATE TABLE test.sag_venona_sample_by_account AS
SELECT
  venona_events.partition_date_utc,
  FROM_UNIXTIME(CAST(venona_events.received__timestamp/1000 as BIGINT),"yyyy-MM-dd HH:mm:ss") formatted_received__timestamp,
  venona_events.received__timestamp,
  venona_events.visit__device__uuid,
  venona_events.message__category,
  venona_events.message__name,
  venona_events.state__view__current_page__page_name,
  venona_events.state__view__current_page__page_section__name,
  venona_events.state__view__current_page__page_sub_section__name,
  venona_events.state__view__current_page__elements__standardized_name,
  venona_events.operation__operation_type,
  -- venona_events.visit__external_apps__app_vendor,
  -- venona_events.visit__external_apps__app_name,
  -- venona_events.visit__external_apps__entitled,
  venona_events.visit__application_details__app_version,
  venona_events.message__triggered_by,
  venona_events.state__name,
  venona_events.state__content__identifiers__tms_series_id,
  venona_events.state__content__identifiers__tms_program_id,
  venona_events.state__content__identifiers__tms_guide_id,
  venona_events.visit__application_details__application_name
FROM prod.venona_events
WHERE
          partition_date_utc = '2018-01-26'
      -- AND state__content__identifiers__tms_guide_id = 'SH025232490000'
      AND visit__application_details__application_name = 'Spectrum Guide'
      AND visit__device__uuid =  '0+DaIEZ7aXnWPH842rPb8g=='
      -- 'usLaG/28fDi1tr1M9NEJ2w==',
      -- 'ZV6UK/eoHI5z2lZYpG6tLQ==')
      ;

DROP TABLE IF EXISTS test.sag_checking_counts;
CREATE TABLE test.sag_checking_counts AS
SELECT
CONCAT_WS ('--',state__content__identifiers__tms_guide_id, message__category, message__name,state__view__current_page__elements__standardized_name, state__view__current_page__page_name, state__view__current_page__page_section__name, message__triggered_by),
COUNT(*)
FROM test.sag_venona_sample_by_account
-- WHERE state__content__identifiers__tms_guide_id = 'SH025232490000'
GROUP BY
CONCAT_WS ('--',state__content__identifiers__tms_guide_id, message__category, message__name,state__view__current_page__elements__standardized_name, state__view__current_page__page_name, state__view__current_page__page_section__name, message__triggered_by)
;






DROP TABLE IF EXISTS test.sag_venona_user_path;
CREATE TABLE test.sag_venona_user_path AS
SELECT
      partition_date_utc,
      visit__application_details__app_version,
      CONCAT_WS (
             '--'
            ,COALESCE(state__content__identifiers__tms_guide_id, 'N/A')
            ,COALESCE(message__category, message__name, 'N/A')
            ,COALESCE(state__view__current_page__elements__standardized_name, 'N/A')
            ,COALESCE(state__view__current_page__page_name, 'N/A')
            ,COALESCE(state__view__current_page__page_section__name, 'N/A')
            ,COALESCE(state__view__current_page__page_sub_section__name, 'N/A')
            ,COALESCE(message__triggered_by, 'N/A')
            ,COALESCE(operation__operation_type, 'N/A')
            )
      AS user_path,
      SIZE(COLLECT_SET(visit__device__uuid)) AS STBs,
      count(*) AS count_of_events
FROM prod.venona_events venona_events
WHERE
          partition_date_utc >= '2018-01-22'
      AND state__content__identifiers__tms_guide_id = 'SH025232490000'
      AND visit__application_details__application_name = 'Spectrum Guide'

GROUP BY
      partition_date_utc,
      visit__application_details__app_version,
      CONCAT_WS (
             '--'
            ,COALESCE(state__content__identifiers__tms_guide_id, 'N/A')
            ,COALESCE(message__category, message__name, 'N/A')
            ,COALESCE(state__view__current_page__elements__standardized_name, 'N/A')
            ,COALESCE(state__view__current_page__page_name, 'N/A')
            ,COALESCE(state__view__current_page__page_section__name, 'N/A')
            ,COALESCE(state__view__current_page__page_sub_section__name, 'N/A')
            ,COALESCE(message__triggered_by, 'N/A')
            ,COALESCE(operation__operation_type, 'N/A')
              )
      ;










-- ============
-- == STEP 1: CREATE A BASE TABLE OF ALL VENONA TAGGING WITH RELAVENT COLUMNS, BROADLY SEARCHING TO SEE IF TAGGING EVER OCCURRED
-- ============
DROP TABLE IF EXISTS test.SAG_venona_tagging;
CREATE TABLE test.SAG_venona_tagging AS

SELECT
      message__name,
      message__category,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name,
      state__view__current_page__page_section__name,
      operation__operation_type
FROM PROD.VENONA_EVENTS
WHERE
          partition_date_utc >= '2017-08-01'
      AND partition_date_utc <= '2018-01-11'
      AND
        (
            visit__application_details__application_name = 'Spectrum Guide'
        OR  visit__application_details__application_type = 'SpecGuide'
        )

GROUP BY
      message__name,
      message__category,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name,
      state__view__current_page__page_section__name,
      operation__operation_type
      ;


-- ============
-- == STEP 2: WORLD BOX STBs BEGIN WITH 34. PERFORM A WILD CARD SERCH FOR THESE STBS FOR THEIR ACTIVITY.
-- ============

DROP TABLE test.SAG_netflix_check_tables;
CREATE TABLE test.SAG_netflix_check_tables AS
SELECT *
FROM prod.VENONA_SG_P2_EVENTS_DENVER
WHERE prod.aes_decrypt(visit__device__uuid) LIKE '34%'
;

SELECT visit__device__uuid FROM test.SAG_netflix_check_tables
WHERE prod.aes_decrypt(visit__device__uuid) LIKE '%3438B77F8FD8%' OR prod.aes_decrypt(visit__device__uuid) LIKE '%3438B77F8FD9%'

-- ============
-- == STEP 3: TAKE AN EXAMPLE OF A MAC WHERE YOU WERE ABLE TO SEE THAT IT NETFLIX IN THE STANDARDIZED NAME.
-- == PULL ALL VENONA RECORDS AND SEARCH EVERY FIELD FOR SOMETHING THAT INDICATES A NETFLIX LAUNCH.
-- ============
SELECT prod.aes_decrypt(visit__device__uuid) unencrypted_visit__device__uuid, *
FROM prod.VENONA_SG_P2_EVENTS_DENVER --> THIS IS SIMPLY A VIEW.
WHERE visit__device__uuid IN ('m+pvJbiwA6ur/o3lt5cJtA==') --> THIS MAC WAS CHOSEN B/C IT MET THE FOLLOWING CONDITION: UPPER(state__view__current_page__page_name) LIKE '%NETFLIX%'
AND partition_date_utc IN ('2018-01-06')





-- ============
-- == STEP 2: CREATE A BASE TABLE OF ALL VENONA STAGGING TAGGING WITH RELAVENT COLUMNS, BROADLY SEARCHING TO SEE IF TAGGING EVER OCCURRED
-- ============
DROP TABLE IF EXISTS test.SAG_events_staging_venona_tagging;
CREATE TABLE test.SAG_events_staging_venona_tagging AS

SELECT
      message__name,
      message__category,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name,
      state__view__current_page__page_section__name,
      operation__operation_type
FROM PROD.VENONA_EVENTS_STAGING
WHERE
          partition_date_utc >= '2017-08-01'
      AND partition_date_utc <= '2017-12-26'
      AND
        (
            visit__application_details__application_name = 'Spectrum Guide'
        OR  visit__application_details__application_type = 'SpecGuide'
        )

GROUP BY
      message__name,
      message__category,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name,
      state__view__current_page__page_section__name,
      operation__operation_type
      ;



1) I can see people went to the appscren. I cannot see if they then clicked on netflix app.


-- ============
-- == APP SCREEN
-- ============
        SpecGuide_appsScreen_pageView:
          description: Apps page showing apps that can be launched through SpecGuide (youtube, netflix, etc.)
          platform: SpecGuide
          expands: [pageView.yaml]
          properties:
            message.triggeredBy: { required: true, valid: [user] }
            message.name: { required: true, valid: [pageView] }
            message.category: { required: true, valid: [navigation] }
            state.view.currentPage.pageName: { required: true, valid: [appsScreen] }
            state.view.currentPage.appSection: { required: true, valid: [apps] }

SELECT
      *
FROM test.SAG_venona_tagging
WHERE
          UPPER(message__name) LIKE '%PAGEVIEW%'
      AND UPPER(state__view__current_page__page_name) LIKE '%APPSSCREEN%'
      ;

-- ============
-- == PAGEVIEWS
-- ============

SpecGuide_NetflixLandingPage_pageView:
  description: View Netflix landing page view
  platform: SpecGuide
  expands: [pageView.yaml]
  properties:
    message.triggeredBy: { required: true, valid: [user] }
    message.name: { required: true, valid: [pageView] }
    message.category: { required: true, valid: [navigation] }
    state.view.currentPage.pageName: { required: true, valid: [landingPageNetflix] }
    state.view.currentPage.appSection: { required: true, valid: [appLobby] }

-- ============
-- == STEP 1: TEST TO DETERMINE IF NETFLIX PAGE VIEWS EXIST
-- ============
SELECT
      *
FROM test.SAG_venona_tagging
WHERE
          UPPER(message__name) LIKE '%PAGEVIEW%'
      AND UPPER(state__view__current_page__page_name) LIKE '%NETFLIX%' --> Use wild card for now

      ;


      SELECT
            *
      FROM test.SAG_venona_tagging
      WHERE
                UPPER(message__name) LIKE '%PAGEVIEW%'
                AND UPPER(state__view__current_page__page_name) LIKE '%APPS%';

-- ============
-- == NETFLIX ACTIONS
-- ============

SpecGuide_selectAction_launchNetflixOrYoutube:
  description: Select Launch on Netflix or Youtube launch page
  platform: SpecGuide
  expands: [selectAction.yaml]
  properties:
    message.name: { required: true, valid: [selectAction] }
    message.category: { required: true, valid: [navigation] }
    message.triggeredBy: { required: true, valid: [user] }
    state.view.currentPage.elements.standardizedName: { required: true, valid: [launchNetflix, launchYoutube] }
    state.view.currentPage.pageSection.name: { required: true, valid: [conversionArea] }
    operation.operationType: { required: true, valid: [buttonClick] }

SpecGuide_selectAction_goBackNetflixOrYoutube:
  description: Select Go Back on Netflix or Youtube launch page
  platform: SpecGuide
  expands: [selectAction.yaml]
  properties:
    message.name: { required: true, valid: [selectAction] }
    message.category: { required: true, valid: [navigation] }
    message.triggeredBy: { required: true, valid: [user] }
    state.view.currentPage.elements.standardizedName: { required: true, valid: [goBackNetflix, goBackYoutube] }
    state.view.currentPage.pageSection.name: { required: true, valid: [conversionArea] }
    operation.operationType: { required: true, valid: [buttonClick] }


SELECT
      *
FROM test.SAG_venona_tagging
WHERE
          UPPER(message__name) LIKE '%SELECTACTION%'
      AND
        (
           UPPER(state__view__current_page__elements__standardized_name) LIKE '%GOBACKNETFLIX%'
        -- OR UPPER(state__view__current_page__elements__standardized_name) LIKE '%GOBACKYOUTUBE%'
        OR UPPER(state__view__current_page__elements__standardized_name) LIKE '%LAUNCHNETFLIX%'
        -- OR UPPER(state__view__current_page__elements__standardized_name) LIKE '%LAUNCHYOUTUBE%'
        OR UPPER(state__view__current_page__elements__standardized_name) LIKE '%NETFLIX%'
        -- OR UPPER(state__view__current_page__elements__standardized_name) LIKE '%YOUTUBE%'
      )
      ;

-- ============
-- == WHAT THE LANDING PAGE FOR NETFLIX SHOULD BE PER YAMLS
-- ============
SELECT *
FROM prod.VENONA_EVENTS
WHERE
          message__name = 'pageView'
      AND message__category = 'navigation'
      AND state__view__current_page__page_name = 'landingPageNetflix'
      AND state__view__current_page__app_section = 'appLobby'
      AND visit__application_details__application_name = 'Spectrum Guide'
      ;












DROP TABLE IF EXISTS test.SAG_venona_tagging_v1;
CREATE TABLE test.SAG_venona_tagging_v1 AS

SELECT
      message__name,
      message__category,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name,
      state__view__current_page__page_section__name,
      operation__operation_type,
      message__triggered_by,
      message__triggered_using,
      state__view__current_page__page_type,
      state__view__current_page__page_view_type,
      state__view__current_page__page_display_type,
      visit__external_apps__app_vendor,
      visit__external_apps__app_name
FROM PROD.VENONA_EVENTS
WHERE
          partition_date_utc >= '2017-08-01'
      AND partition_date_utc <= '2017-10-01'
      AND
        (
            visit__application_details__application_name = 'Spectrum Guide'
        OR  visit__application_details__application_type = 'SpecGuide'
        )

GROUP BY
      message__name,
      message__category,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name,
      state__view__current_page__page_section__name,
      operation__operation_type,
      message__triggered_by,
      message__triggered_using,
      state__view__current_page__page_type,
      state__view__current_page__page_view_type,
      state__view__current_page__page_display_type,
      visit__external_apps__app_vendor,
      visit__external_apps__app_name
      ;



DROP TABLE IF EXISTS test.SAG_venona_tagging_v2;
CREATE TABLE test.SAG_venona_tagging_v2 AS

SELECT
      message__name,
      message__category,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name,
      state__view__current_page__page_section__name,
      operation__operation_type,
      message__triggered_by,
      message__triggered_using,
      state__view__current_page__page_type,
      state__view__current_page__page_view_type,
      state__view__current_page__page_display_type,
      visit__external_apps__app_vendor,
      visit__external_apps__app_name
FROM PROD.VENONA_EVENTS
WHERE
          partition_date_utc >= '2017-10-01'
      AND partition_date_utc <='2017-12-22'
      AND
        (
            visit__application_details__application_name = 'Spectrum Guide'
        OR  visit__application_details__application_type = 'SpecGuide'
        )

GROUP BY
      message__name,
      message__category,
      state__view__current_page__page_name,
      state__view__current_page__app_section,
      state__view__current_page__elements__standardized_name,
      state__view__current_page__page_section__name,
      operation__operation_type,
      message__triggered_by,
      message__triggered_using,
      state__view__current_page__page_type,
      state__view__current_page__page_view_type,
      state__view__current_page__page_display_type,
      visit__external_apps__app_vendor,
      visit__external_apps__app_name
      ;

CREATE TABLE test.SAG_venona_tagging_v2 AS
SELECT * FROM SAG_venona_tagging_v1
UNION
SELECT * FROM SAG_venona_tagging_v2
;





SELECT
      *
FROM test.SAG_venona_tagging
WHERE
          --UPPER(message__name) LIKE '%PAGEVIEW%'
          --UPPER(message__name) LIKE '%SELECTACTION%'
          --AND message__category = 'navigation'
          --AND operation__operation_type = 'buttonClick'
          UPPER(state__view__current_page__elements__standardized_name) LIKE '%NETFLIX%'



---------------------------
SELECT prod.aes_decrypt(visit__device__uuid) unencrypted_visit__device__uuid
FROM test.sag_netflix_check_tables stats
WHERE partition_date_denver >= '2018-01-09'
AND prod.aes_decrypt(visit__device__uuid) LIKE '3438B78%'
GROUP BY prod.aes_decrypt(visit__device__uuid)

AND prod.aes_decrypt(visit__device__uuid) LIKE '3438B77F8FD8' OR  prod.aes_decrypt(visit__device__uuid) LIKE '3438B77F8FD9'

SELECT prod.aes_decrypt(visit__device__uuid) unencrypted_visit__device__uuid, *
FROM prod.VENONA_SG_P2_EVENTS_DENVER
--WHERE UPPER(state__view__current_page__elements__standardized_name) LIKE '%NETFLIX%'
WHERE visit__device__uuid IN ('m+pvJbiwA6ur/o3lt5cJtA==')
AND partition_date_utc IN ('2018-01-06')
, 'xOOjnLoKmu86HHZ9zEM34A==')


CREATE TABLE test.SAG_netflix_check_tables AS
SELECT *
FROM prod.VENONA_SG_P2_EVENTS_DENVER
WHERE prod.aes_Decrypt(visit__device__uuid) LIKE '%34%'

SELECT message__name, state__view__current_page__elements__standardized_name,
FROM prod.VENONA_SG_P2_EVENTS_DENVER
--WHERE UPPER(state__view__current_page__elements__standardized_name) LIKE '%NETFLIX%'
 WHERE visit__device__uuid IN ('m+pvJbiwA6ur/o3lt5cJtA==', 'xOOjnLoKmu86HHZ9zEM34A==')
