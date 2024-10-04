CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.asp_adhoc_sspp_events (
  visit__visit_id string,
  customer string,
  domain string,
  message__name string,
  application__api__api_name string,
  application__api__path string,
  application__api__response_code string,
  application__api__response_text string,
  application__api__service_result string,
  application__error__client_error_code string,
  application__error__error_code string,
  application__error__error_extras map<string,string>,
  application__error__enc_error_extras map<string,string>,
  application__error__error_message string,
  application__error__error_type string,
  message__category string,
  message__context string,
  message__event_case_id string,
  message__feature__completed_steps array<string>,
  message__feature__current_step int,
  message__feature__feature_name string,
  message__feature__feature_step_changed boolean,
  message__feature__feature_step_name string,
  message__feature__featuretype string,
  message__feature__number_of_steps int,
  message__feature__previous_step string,
  message__feature__transaction_id string,
  message__sequence_number int,
  message__triggered_by string,
  operation__additional_information string,
  operation__billing__payment_amount_usd double,
  operation__billing__payment_date string,
  operation__billing__payment_due_date string,
  operation__billing__payment_method string,
  operation__operation_type string,
  operation__settings_updates map<string,string>,
  operation__success boolean,
  operation__toggle_state boolean,
  operation__user_entry__category string,
  operation__user_entry__enc_account_number string,
  operation__user_entry__enc_feedback string,
  operation__user_entry__enc_feedback_id string,
  operation__user_entry__enc_phone_number string,
  operation__user_entry__enc_text string,
  operation__user_entry__entry_type string,
  operation__user_entry__numeric int,
  operation__user_entry__question_id int,
  operation__user_entry__question_type string,
  operation__user_entry__str_value string,
  operation__user_entry__survey_id string,
  operation__user_preferences_selections array<string>,
  state__content__content_class string,
  state__content__details__episode_number string,
  state__content__details__genres array<string>,
  state__content__details__season_number string,
  state__content__identifiers__tms_program_id string,
  state__content__programmer__linear__channel_name string,
  state__content__stream__playback_id string,
  state__content__stream__playback_type string,
  state__search__number_of_search_results int,
  state__search__results_ms int,
  state__search__search_id string,
  state__search__search_results__facet array<string>,
  state__search__search_results__id array<string>,
  state__search__search_results__id_type array<string>,
  state__search__search_results__name array<string>,
  state__search__search_results__url array<string>,
  state__search__search_type string,
  state__search__text string,
  state__view__current_page__additional_information string,
  state__view__current_page__app_section string,
  state__view__current_page__current_balance_amount double,
  state__view__current_page__elements__element_index int,
  state__view__current_page__elements__element_int_value int,
  state__view__current_page__elements__element_string_value string,
  state__view__current_page__elements__element_type string,
  state__view__current_page__elements__element_url string,
  state__view__current_page__elements__number_of_items int,
  state__view__current_page__elements__selected_options array<string>,
  state__view__current_page__elements__standardized_name string,
  state__view__current_page__elements__ui_name string,
  state__view__current_page__page_id string,
  state__view__current_page__page_name string,
  state__view__current_page__page_section__name string,
  state__view__current_page__page_sequence_number int,
  state__view__current_page__page_sub_section__name string,
  state__view__current_page__page_title string,
  state__view__current_page__enc_page_title string,
  state__view__current_page__dec_page_title string,
  state__view__current_page__page_type string,
  state__view__current_page__page_url string,
  state__view__current_page__enc_page_url string,
  state__view__current_page__past_due_amount double,
  state__view__current_page__render_details__cold_page_fully_loaded_time int,
  state__view__current_page__render_details__cold_page_interactive_load_time int,
  state__view__current_page__render_details__fully_rendered_ms bigint,
  state__view__current_page__render_details__fully_rendered_timestamp bigint,
  state__view__current_page__render_details__is_lazy_load boolean,
  state__view__current_page__render_details__partial_rendered_ms bigint,
  state__view__current_page__render_details__partial_rendered_timestamp bigint,
  state__view__current_page__render_details__render_init_timestamp bigint,
  state__view__current_page__render_details__view_rendered_status string,
  state__view__current_page__sort_and_filter__applied_filters array<string>,
  state__view__current_page__sort_and_filter__applied_sorts array<string>,
  state__view__current_page__total_amount_due double,
  state__view__current_page__ui_responsive_breakpoint int,
  state__view__current_page__user_full_journey array<string>,
  state__view__current_page__user_journey array<string>,
  state__view__current_page__user_sub_journey array<string>,
  state__view__currentpage__campaign__page_view_campaign_id string,
  state__view__modal__modal_type string,
  state__view__modal__name string,
  state__view__modal__text string,
  state__view__previous_page__app_section string,
  state__view__previous_page__page_name string,
  state__view__previous_page__page_section__name string,
  state__view__previous_page__page_type string,
  state__view__previous_page__page_url string,
  state__view__previous_page__enc_page_url string,
  state__view__previous_page__sort_and_filter__applied_filters array<string>,
  visit__account__configuration_factors string,
  visit__account__details__auto_pay_status string,
  visit__account__details__billing_status string,
  visit__account__details__mso string,
  visit__account__details__service_subscriptions map<string,string>,
  visit__account__details__statement_method string,
  visit__account__enc_account_billing_division string,
  visit__account__enc_account_billing_division_id string,
  visit__account__enc_account_billing_id string,
  visit__account__enc_account_number string,
  visit__account__enc_mobile_account_number string,
  visit__activated_experiment_uuids array<string>,
  visit__activated_experiments map<string,string>,
  visit__additional_information string,
  visit__analytics__library_version string,
  visit__analytics__requirements_source string,
  visit__analytics__requirements_version string,
  visit__application_details__app_version string,
  visit__application_details__application_name string,
  visit__application_details__application_type string,
  visit__application_details__platform_type string,
  visit__application_details__referrer_app__application_name string,
  visit__application_details__referrer_app__application_type string,
  visit__application_details__referrer_app__visit_id string,
  visit__application_details__referrer_link string,
  visit__application_details__enc_referrer_link  string,
  visit__application_details__technology_type string,
  visit__application_details__venona_requirements_version string,
  visit__application_details__venona_version string,
  visit__connection__connection_type string,
  visit__connection__network_status string,
  visit__device__browser__name string,
  visit__device__browser__user_agent string,
  visit__device__browser__version string,
  visit__device__device_form_factor string,
  visit__device__device_type string,
  visit__device__enc_uuid string,
  visit__device__manufacturer string,
  visit__device__model string,
  visit__device__operating_system string,
  visit__experiment_uuids array<string>,
  visit__location__country_code string,
  visit__location__country_name string,
  visit__location__enc_city string,
  visit__location__enc_latitude string,
  visit__location__enc_longitude string,
  visit__location__enc_zip_code string,
  visit__location__metro_code string,
  visit__location__region string,
  visit__location__region_name string,
  visit__location__state string,
  visit__location__status string,
  visit__location__time_zone string,
  visit__login__failed_attempts int,
  visit__login__logged_in boolean,
  visit__login__login_completed_timestamp bigint,
  visit__login__login_duration_ms int,
  visit__previous_visit_id string,
  visit__screen_resolution string,
  visit__variant_uuids array<string>,
  visit__visit_start_timestamp bigint,
  message__timestamp bigint,
  received__timestamp bigint,
  partition_date_hour_utc string,
  partition_date_utc string,
  mso string,
  os_name string
)
PARTITIONED BY (denver_date string)
STORED AS PARQUET
LOCATION '${s3_location}'
TBLPROPERTIES ('parquet.compression'='SNAPPY')

;
