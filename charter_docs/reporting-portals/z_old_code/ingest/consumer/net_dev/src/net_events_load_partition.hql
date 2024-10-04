USE ${env:ENVIRONMENT};

INSERT INTO TABLE net_dev_events PARTITION (partition_date_utc)
SELECT 
  domain,
  customer,
  received_timestamp,
  unfinished_created_timestamp,
  finished_created_timestamp,
  visit__visit_id,
  visit__previous_visit_id,
  visit__visit_start_timestamp,
  visit__visit_startup_time,
  visit__referrer_link,
  visit__device__type,
  visit__device__model, 
  visit__device__operating_system,
  visit__device__enc_uuid,
  visit__device__available_storage,
  visit__device__browser__name,
  visit__device__browser__version,
  visit__device__linked_device__dvr_capable,
  visit__device__linked_device__name,
  visit__device__enc_linked_device__id,
  visit__login__failed_attempts AS STRING,
  visit__login__login_duration,
  visit__login__login_completed_timestamp,
  visit__video_zone__details,
  ${env:ENVIRONMENT}.aes_encrypt256(visit__isp__enc_ip_address),
  visit__isp__location_id,
  visit__isp__organization,
  visit__isp__isp,
  visit__isp__status,
  visit__connection__type,
  visit__connection__network_status,
  visit__connection__online,
  visit__connection__speed_test__output,
  visit__connection__speed_test__start_time,
  visit__connection__speed_test__end_time,
  visit__account__id,
  visit__account__enc_account_number,
  visit__account__address__address_type,
  visit__account__address__city,
  visit__account__address__state,
  visit__account__address__zip_code,
  visit__account__details,
  visit__account__subscription__service_level,
  visit__account__status,
  visit__user__enc_id,
  visit__user__role,
  visit__user__is_test_user,
  visit__user__details,
  visit__user__status,
  visit__location__country_code,
  visit__location__country_name,
  visit__location__region,
  visit__location__region_name,
  visit__location__enc_city,
  visit__location__state,
  visit__location__enc_zip_code,
  visit__location__enc_latitude,
  visit__location__enc_longitude,
  visit__location__metro_code,
  visit__location__timezone,
  visit__location__status,
  visit__app_version,
  visit__library_version,
  visit__app_name,
  visit__environment,
  visit__resolution,
  visit__settings,
  application__error__type,
  application__error__code,
  application__error__details,
  application__api__response_code,
  ${env:ENVIRONMENT}.aes_encrypt256(application__api__response_text),
  application__api__timeout,
  application__api__response_time_in_ms,
  application__api__host,
  application__api__api_name,
  application__api__http_verb,
  application__api__query_parameters,
  message__category,
  message__name,
  message__sequence_number,
  message__timestamp,
  message__initiated_by,
  message__feature__name,
  message__feature__current_step,
  message__feature__number_of_steps,
  state__name,
  state__entry_timestamp,
  state__entry_content_position,
  state__current_content_position,
  state__previous_state__name,
  state__previous_state__entry_timestamp,
  state__previous_state__entry_content_position,
  state__previous_state__exit_timestamp,
  state__previous_state__exit_content_position,
  state__playback__playback_started_timestamp,
  state__playback__tune_timestamp,
  state__playback__heart_beat__time_elapsed_in_seconds,
  state__playback__heart_beat__content_elapsed_in_seconds,
  state__playback__bit_rate__current_bit_rate,
  state__playback__bit_rate__content_elapsed_at_current_bit_rate,
  state__playback__bit_rate__previous_bit_rate,
  state__playback__bit_rate__content_elapsed_at_previous_bit_rate,
  state__playback__trick_play__scrub_speed,
  state__playback__trick_play__scrub_type,
  state__playback__trick_play__scrub_start_position,
  state__playback__trick_play__scrub_end_position,
  state__content__stream__content_uri,
  state__content__stream__type,
  state__content__stream__content_format,
  state__content__stream__drm_type,
  state__content__stream__scrubbing_capability,
  state__content__stream__closed_captioning_capable,
  state__content__stream__bookmark_position,
  state__content__stream__entitled,
  state__content__stream__parentally_blocked,
  state__content__stream__favorited,
  state__content__stream__view_id,
  state__content__stream__price,
  state__content__stream__license_type,
  state__content__stream__start_timestamp,
  state__content__stream__end_timestamp,
  state__content__identifiers__id,
  state__content__identifiers__id_type,
  state__content__identifiers__series_id_type,
  state__content__identifiers__series_id,
  state__content__identifiers__recording_timestamp,
  state__content__details__type,
  state__content__details__title,
  state__content__details__episode_number,
  state__content__details__season_number,
  state__content__details__available_date,
  state__content__details__closed_captioned,
  state__content__details__long_form,
  state__content__details__runtime,
  state__content__details__expiration_date,
  state__content__details__original_air_date,
  state__content__details__original_network_name,
  state__content__details__rating,
  state__content__details__description,
  state__content__details__year,
  state__content__details__episode_title,
  state__content__details__hd,
  state__content__details__actors,
  state__content__details__genres,
  state__content__programmer__callsign,
  state__content__programmer__id,
  state__content__programmer__vod__provider_id,
  state__content__programmer__vod__provider_name,
  state__content__programmer__vod__product,
  state__content__programmer__linear__channel_name,
  state__content__programmer__linear__network_name,
  state__content__programmer__linear__channel_category,
  state__content__programmer__linear__channel_number,
  state__ad__id,
  state__ad__title,
  state__ad__number,
  state__ad__ad_start_timestamp,
  state__ad__break_start_timestamp,
  state__ad__break_start_position,
  state__ad__break_length_seconds,
  state__ad__break_number,
  state__ad__commodity_code,
  state__ad__vast,
  state__ad__campaign,
  state__view__current_page__name,
  state__view__current_page__section,
  state__view__current_page__sub_section,
  state__view__current_page__page_id,
  state__view__current_page__page_load_time,
  state__view__current_page__type,
  state__view__current_page__display_type,
  state__view__current_page__sort_and_filter__applied_sorts,
  state__view__current_page__sort_and_filter__applied_filters,
  state__view__current_page__search_text,
  state__view__current_page__page_number,
  state__view__current_page__elements__name,
  state__view__current_page__elements__number_of_items,
  state__view__current_page__elements__type,
  state__view__current_page__elements__parentally_blocked_items,
  state__view__current_page__elements__entitled_items,
  state__view__current_page__settings,
  state__view__previous_page__name,
  state__view__previous_page__section,
  state__view__previous_page__sub_section,
  state__view__previous_page__page_id,
  state__view__previous_page__page_load_time,
  state__view__modal__name,
  state__view__modal__text,
  state__view__modal__type,
  state__view__modal__load_time,
  state__download__content_window,
  state__download__quality,
  state__download__watched,
  operation__toggle_state,
  operation__type,
  operation__success,
  operation__selected__index,
  operation__selected__view_element_name,
  operation__user_entry__enc_text,
  operation__user_entry__type,
  epoch_converter(cast(message__timestamp*1000 as bigint),'UTC') as partition_date_utc
FROM
  ${env:TMP_db}.net_DEV_events_no_part;

