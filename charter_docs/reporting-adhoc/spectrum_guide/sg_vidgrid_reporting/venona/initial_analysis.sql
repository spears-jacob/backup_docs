-- ============
-- == RESULT SET
-- ============
SELECT * FROM test.sa_vid_grid_values;

SELECT
SIZE(COLLECT_SET(IF(array_contains(menu_guide_states,'standardGuide') = TRUE, visit__device__uuid, NULL))) AS stbs_standardguide_count,
SIZE(COLLECT_SET(IF(array_contains(menu_guide_states,'videoGuide') = TRUE, visit__device__uuid, NULL))) AS stbs_videoguide_count,
SIZE(COLLECT_SET(IF((array_contains(menu_guide_states,'standardGuide') = TRUE AND array_contains(menu_guide_states,'videoGuide') = TRUE) , visit__device__uuid, NULL))) AS stbs_standardguide_and_videoguide_count,
SIZE(COLLECT_SET(visit__device__uuid)) AS stbs_active_count
FROM test.sa_vid_grid_values
;

-- ============
-- == TESTING TO SEE IF GUIDE PAGE VIEWS ARE TIED TO A GUIDE SCREEN TYPE
-- ============
SELECT
      partition_date_utc,
      visit__application_details__application_name,
      state__view__current_page__page_name,
      -- state__view__current_page__page_display_type, --> ('videoGuide' , 'standardGuide') --> DIFFERENT GUIDE OPTIONS
      state__view__current_page__elements__standardized_name, --> only when a customer make a setting select, not a page view --> ('showStandardGuide', 'showVideoGuide')
      message__category,
      message__name,
      state__view__current_page__page_display_type,
      COUNT(*)

FROM prod.venona_events
WHERE
          partition_date_utc =  '2017-10-01'
      AND visit__application_details__application_name = 'Spectrum Guide'
      AND state__view__current_page__page_name IN ('guide') --> guide screen
      --AND state__view__current_page__page_display_type IN ('videoGuide' , 'standardGuide') --> DIFFERENT GUIDE OPTIONS
      AND message__name IN ('pageView') --> user is viewing the page
      AND visit__device__uuid IS NOT NULL -->
      --AND visit__device__uuid IN ('z6IXI/sg59QDvVkHE8hw1Q==') --> sample mac
      --AND state__view__current_page__elements__standardized_name --> only when a customer make a setting select, not a page view --> ('showStandardGuide', 'showVideoGuide')

GROUP BY
      partition_date_utc,
      visit__application_details__application_name,
      state__view__current_page__page_name,
      -- state__view__current_page__page_display_type,
      state__view__current_page__elements__standardized_name,
      message__category,
      message__name,
      state__view__current_page__page_display_type
      ;
-- ============
-- == TABLE BUILD
-- ============
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

DROP TABLE IF EXISTS test.sa_vid_grid_values;
CREATE TABLE test.sa_vid_grid_values AS

SELECT
      visit__device__uuid,
      COLLECT_SET(state__view__current_page__page_display_type) AS menu_guide_states


FROM prod.venona_events
WHERE
          partition_date_utc =  '2017-10-01'
      AND visit__application_details__application_name = 'Spectrum Guide'
      AND state__view__current_page__page_name IN ('guide', 'guideOptions') --> THE TWO PAGES LINKED TO GUIDE TYPE OPTIONS
      AND state__view__current_page__page_display_type IN ('videoGuide' , 'standardGuide') --> DIFFERENT GUIDE OPTIONS
      AND message__name IN ('pageView')  --> DENOTES THAT A CUSTOMERS VIEWED THAT SPECIFIC GUIDE TYPE, WHICH IMPLIES A CERTAIN GUIDE TYPE WAS SET
      --AND state__view__current_page__elements__standardized_name IN ('showStandardGuide', 'showVideoGuide')
      --AND message__name IN ('selectAction') indicates that a certain guide type was set as a setting on a particular day
      AND visit__device__uuid IN
                      (
                  'RWjHd4Ts/ou8IVyfjMMbGQ==', --> ('videoGuide' , 'standardGuide')
                  'z6IXI/sg59QDvVkHE8hw1Q==' --> ('videoGuide' , 'standardGuide')
                  -- '+0BHfPCHXFDryZw8GRpJBA==', --> ('showStandardGuide', 'showVideoGuide')
                  -- '+1LBzGGsLh7R9mIMbaCxiQ==', --> ('showStandardGuide', 'showVideoGuide')
                  -- '+3NudJQEAyF77TdGLByyyw==' --> ('showStandardGuide', 'showVideoGuide')
                      )
GROUP BY
      visit__device__uuid
         ;
