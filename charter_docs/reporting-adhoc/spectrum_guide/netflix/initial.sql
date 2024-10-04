SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

-- ============
-- == CHECK FOR NETFLIX TAGGING BASED ON TMS PROGRAM ID THAT HAS BEEN IDENTIFIED AS NETFLIX
-- ============
DROP TABLE IF EXISTS test.SAG_netflix_tms_id_check;
CREATE TABLE test.SAG_netflix_tms_id_check AS

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
        (state__view__content__identifiers__tms_guide_id = 'SH025232490000' --> this is the tms_id for netflix
            OR state__content__identifiers__tms_series_id = 'SH025232490000'
            OR state__content__identifiers__tms_program_id = 'SH025232490000'
        )
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
