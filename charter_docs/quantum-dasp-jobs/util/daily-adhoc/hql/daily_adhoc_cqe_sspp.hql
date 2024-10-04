USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
DROP FUNCTION IF EXISTS epoch_converter; CREATE FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/hadoop-libs-hive-1.0.10.jar;
DROP FUNCTION IF EXISTS aes_decrypt256; CREATE TEMPORARY FUNCTION aes_decrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesDecrypt256';

INSERT OVERWRITE TABLE asp_adhoc_sspp_events PARTITION (denver_date)
SELECT  visit__visit_id,
        customer,
        domain,
        message__name,
        application__api__api_name,
        application__api__path,
        application__api__response_code,
        application__api__response_text,
        application__api__service_result,
        application__error__client_error_code,
        application__error__error_code,
        application__error__error_extras,
        application__error__enc_error_extras,
        application__error__error_message,
        application__error__error_type,
        message__category,
        message__context,
        message__event_case_id,
        message__feature__completed_steps,
        message__feature__current_step,
        message__feature__feature_name,
        message__feature__feature_step_changed,
        message__feature__feature_step_name,
        message__feature__featuretype,
        message__feature__number_of_steps,
        message__feature__previous_step,
        message__feature__transaction_id,
        message__sequence_number,
        message__triggered_by,
        operation__additional_information,
        operation__billing__payment_amount_usd,
        operation__billing__payment_date,
        operation__billing__payment_due_date,
        operation__billing__payment_method,
        operation__operation_type,
        operation__settings_updates,
        operation__success,
        operation__toggle_state,
        operation__user_entry__category,
        operation__user_entry__enc_account_number,
        operation__user_entry__enc_feedback,
        operation__user_entry__enc_feedback_id,
        operation__user_entry__enc_phone_number,
        operation__user_entry__enc_text,
        operation__user_entry__entry_type,
        operation__user_entry__numeric,
        operation__user_entry__question_id,
        operation__user_entry__question_type,
        operation__user_entry__str_value,
        operation__user_entry__survey_id,
        operation__user_preferences_selections,
        state__content__content_class,
        state__content__details__episode_number,
        state__content__details__genres,
        state__content__details__season_number,
        state__content__identifiers__tms_program_id,
        state__content__programmer__linear__channel_name,
        state__content__stream__playback_id,
        state__content__stream__playback_type,
        state__search__number_of_search_results,
        state__search__results_ms,
        state__search__search_id,
        state__search__search_results__facet,
        state__search__search_results__id,
        state__search__search_results__id_type,
        state__search__search_results__name,
        state__search__search_results__url,
        state__search__search_type,
        state__search__text,
        state__view__current_page__additional_information,
        state__view__current_page__app_section,
        state__view__current_page__current_balance_amount,
        state__view__current_page__elements__element_index,
        state__view__current_page__elements__element_int_value,
        state__view__current_page__elements__element_string_value,
        state__view__current_page__elements__element_type,
        state__view__current_page__elements__element_url,
        state__view__current_page__elements__number_of_items,
        state__view__current_page__elements__selected_options,
        state__view__current_page__elements__standardized_name,
        state__view__current_page__elements__ui_name,
        state__view__current_page__page_id,
        state__view__current_page__page_name,
        state__view__current_page__page_section__name,
        state__view__current_page__page_sequence_number,
        state__view__current_page__page_sub_section__name,
        state__view__current_page__page_title,
        state__view__current_page__enc_page_title,
        CASE WHEN state__view__current_page__dec_page_title is not NULL
              THEN state__view__current_page__dec_page_title
             WHEN (state__view__current_page__page_name = 'supportArticle'
                OR state__view__current_page__page_name = 'onlineForm')
              THEN aes_decrypt256(state__view__current_page__enc_page_title, 'aes256')
             ELSE NULL
        END AS state__view__current_page__dec_page_title,
        state__view__current_page__page_type,
        state__view__current_page__page_url,
        state__view__current_page__enc_page_url,
        state__view__current_page__past_due_amount,
        state__view__current_page__render_details__cold_page_fully_loaded_time,
        state__view__current_page__render_details__cold_page_interactive_load_time,
        state__view__current_page__render_details__fully_rendered_ms,
        state__view__current_page__render_details__fully_rendered_timestamp,
        state__view__current_page__render_details__is_lazy_load,
        state__view__current_page__render_details__partial_rendered_ms,
        state__view__current_page__render_details__partial_rendered_timestamp,
        state__view__current_page__render_details__render_init_timestamp,
        state__view__current_page__render_details__view_rendered_status,
        state__view__current_page__sort_and_filter__applied_filters,
        state__view__current_page__sort_and_filter__applied_sorts,
        state__view__current_page__total_amount_due,
        state__view__current_page__ui_responsive_breakpoint,
        state__view__current_page__user_full_journey,
        state__view__current_page__user_journey,
        state__view__current_page__user_sub_journey,
        state__view__currentpage__campaign__page_view_campaign_id,
        state__view__modal__modal_type,
        state__view__modal__name,
        state__view__modal__text,
        state__view__previous_page__app_section,
        state__view__previous_page__page_name,
        state__view__previous_page__page_section__name,
        state__view__previous_page__page_type,
        state__view__previous_page__page_url,
        state__view__previous_page__enc_page_url,
        state__view__previous_page__sort_and_filter__applied_filters,
        visit__account__configuration_factors,
        visit__account__details__auto_pay_status,
        visit__account__details__billing_status,
        visit__account__details__mso,
        visit__account__details__service_subscriptions,
        visit__account__details__statement_method,
        visit__account__enc_account_billing_division,
        visit__account__enc_account_billing_division_id,
        visit__account__enc_account_billing_id,
        visit__account__enc_account_number,
        visit__account__enc_mobile_account_number,
        visit__activated_experiment_uuids,
        visit__activated_experiments,
        visit__additional_information,
        visit__analytics__library_version,
        visit__analytics__requirements_source,
        visit__analytics__requirements_version,
        visit__application_details__app_version,
        visit__application_details__application_name,
        visit__application_details__application_type,
        visit__application_details__platform_type,
        visit__application_details__referrer_app__application_name,
        visit__application_details__referrer_app__application_type,
        visit__application_details__referrer_app__visit_id,
        visit__application_details__referrer_link,
        visit__application_details__enc_referrer_link,
        visit__application_details__technology_type,
        visit__application_details__venona_requirements_version,
        visit__application_details__venona_version,
        visit__connection__connection_type,
        visit__connection__network_status,
        visit__device__browser__name,
        visit__device__browser__user_agent,
        visit__device__browser__version,
        visit__device__device_form_factor,
        visit__device__device_type,
        visit__device__enc_uuid,
        visit__device__manufacturer,
        visit__device__model,
        visit__device__operating_system,
        visit__experiment_uuids,
        visit__location__country_code,
        visit__location__country_name,
        visit__location__enc_city,
        visit__location__enc_latitude,
        visit__location__enc_longitude,
        visit__location__enc_zip_code,
        visit__location__metro_code,
        visit__location__region,
        visit__location__region_name,
        visit__location__state,
        visit__location__status,
        visit__location__time_zone,
        visit__login__failed_attempts,
        visit__login__logged_in,
        visit__login__login_completed_timestamp,
        visit__login__login_duration_ms,
        visit__previous_visit_id,
        visit__screen_resolution,
        visit__variant_uuids,
        visit__visit_start_timestamp,
        message__timestamp,
        received__timestamp,
        partition_date_hour_utc,
        partition_date_utc,
        CASE
          WHEN  visit__account__details__mso IN ('CHARTER','CHTR','"CHTR"')       THEN 'L-CHTR'
          WHEN  visit__account__details__mso IN ('TWC','"TWC"', 'NONECAPTURED')   THEN 'L-TWC'
          WHEN  visit__account__details__mso IN ('BH','BHN','"BHN"')              THEN 'L-BHN'
          WHEN (visit__account__details__mso IN ('','unknown','"NONE"', 'NONE')
                OR visit__account__details__mso IS NULL )                         THEN 'MSO-MISSING'
          ELSE visit__account__details__mso
        END AS mso,
        CASE
          WHEN visit__device__operating_system RLIKE '.*Windows.*' THEN 'Windows'
          WHEN visit__device__operating_system RLIKE '.*Mac.*' THEN 'Mac'
          WHEN (visit__device__operating_system RLIKE '.*iOS.*' OR visit__device__operating_system RLIKE '.*iPhone.*')  THEN 'iOS'
          WHEN visit__device__operating_system RLIKE '.*Android.*' THEN 'Android'
          WHEN visit__device__operating_system RLIKE '.*Chrome.*' THEN 'Chrome OS'
          WHEN visit__device__operating_system RLIKE '.*Linux.*' THEN 'Linux'
          WHEN visit__device__operating_system RLIKE '.*Ubuntu.*' THEN 'Ubuntu'
          ELSE 'OTHER'
        END AS os_name,
        epoch_converter(received__timestamp, 'America/Denver') AS denver_date
FROM `${env:CQE_SSPP}`
WHERE partition_date_hour_utc >= '${env:START_DATE_TZ}'
AND   partition_date_hour_utc <  '${env:END_DATE_TZ}'
AND   visit__application_details__application_name
  IN ('SpecNet',
      'SMB',
      'MySpectrum',
      'IDManagement',
      'PrivacyMicrosite',
      'SpectrumCommunitySolutions'
     )
AND   message__name <> 'apiCall'
;
