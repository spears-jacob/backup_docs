set job.priority HIGH;
set output.compression.enabled true;
set mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
set pig.exec.mapPartAgg true; -- for optimizing the group by statements
set pig.cachedbag.memusage 0.35; -- increased the memory usage of the pig script by 15%. Previously it was 20% and now it is 35%

raw_sbnet_de = LOAD '$TMP_db.sbnet_dev_denorm' USING org.apache.hive.hcatalog.pig.HCatLoader();
raw_sbnet_calc = LOAD '$TMP_db.sbnet_dev_calc' USING org.apache.hive.hcatalog.pig.HCatLoader();

raw_sbnet = JOIN raw_sbnet_de BY unique_id LEFT OUTER, raw_sbnet_calc BY unique_id;

raw_sbnet = FILTER raw_sbnet BY post_cust_hit_time_gmt IS NOT NULL;

sbnet_events = foreach raw_sbnet generate
      'video' as domain:chararray,
      'Charter Communications' as customer:chararray,
      null as received_timestamp:long,
      null as unfinished_created_timestamp:long,
      null as finished_created_timestamp:long,
      --visit
      visit_id as visit__visit_id:chararray,
      previous_visit_id as visit__previous_visit_id:chararray,
      raw_sbnet_calc::visit_start_timestamp as visit__visit_start_timestamp:long,
      null as visit__visit_startup_time_ms:int,
      --visit.application_statistics
      null as visit__application_statistics__days_since_last_upgrade:int,
      null as visit__application_statistics__launches:int,
      null as visit__application_statistics__days_since_last_use:int,
      null as visit__application_statistics__launches_since_upgrade:int,
      null as visit__application_statistics__days_since_first_use:int,
      resolution as visit__screen_resolution:chararray,
      null as visit__settings:map[chararray],
      ((post_prop8 is not null)?post_prop8:((post_prop16 is not null)?post_prop16:((post_prop17 is not null)?post_prop17:post_evar8))) as visit__application_details__referrer_link:chararray,
      null as visit__application_details__application_name:chararray,
      null as visit__application_details__application_type:chararray,
      null as visit__application_details__app_version:chararray,
      null as visit__application_details__venona_version:chararray,
      null as visit__application_details__environment:map[chararray],
      --visit.device
      null as visit__device__vendor_id:chararray,
      null as visit__device__device_form_factor:chararray,
      post_event_list as visit__device__device_type:chararray, --get all events here 2017-12-19
      null as visit__device__model:chararray,
      os as visit__device__operating_system:chararray,
      (hit_source!=5?(hit_source!=7?(hit_source!=8?(hit_source!=9?CONCAT((chararray)post_visid_high,(chararray)post_visid_low):''):''):''):'') as visit__device__enc_uuid:chararray,
      null as visit__device__available_storage:chararray,
      null as visit__device__device_build:chararray,
      null as visit__device__stb_is_dvr:boolean,
      null as visit__device__stb_name:chararray,
      null as visit__device__stb_device_id:chararray,
      null as visit__device__stb_old_name:chararray,
      --visit.device.browser
      browser as visit__device__browser__name:chararray,
      user_agent as visit__device__browser__user_agent:chararray,
      ((browser is not null)?browser:user_agent) as visit__device__browser__version:chararray,
      --visit.device.linked_device
      null as visit__device__linked_device__dvr_capable:chararray,
      null as visit__device__linked_device__name:chararray,
      null as visit__device__enc_linked_device__id:chararray,
      --visit.login
      (int)((post_page_event_var2 == 'Sign In' and post_evar26 matches '*success*')? 1:0) as visit__login__failed_attempts:int,
      null as visit__login__login_duration_ms:double,
      (((post_page_event_var2 == 'Sign In' AND post_prop26 == 'success') OR (post_prop27 matches '*"BehindTheModem":true*') )? post_cust_hit_time_gmt:null) as visit__login__login_completed_timestamp:long,
      --visit.video_zone
      null as visit__video_zone__details:map[chararray],
      null as visit__video_zone__division:chararray,
      null as visit__video_zone__lineup:chararray,
      --visit.isp
      ip as visit__isp__enc_ip_address:chararray,
      null as visit__isp__location_id:chararray,
      null as visit__isp__organization:chararray,
      domain as visit__isp__isp:chararray,
      null as visit__isp__status:chararray,
      --visit.connection
      connection_type as visit__connection__connection_type:chararray,
      null as visit__connection__network_status:chararray,
      null as visit__connection__network_cell_carrier:chararray,
      null as visit__connection__network_cell_network_type:chararray,

      --visit.connection.speed_test
      null as visit__connection__speed_test__output:chararray,
      null as visit__connection__speed_test__start_timestamp:long,
      null as visit__connection__speed_test__end_timestamp:long,
      --visit.account

      ((post_prop1 is not null)?post_prop1:post_evar1) as visit__account__enc_account_number:chararray,
      --visit.account.address
      null as visit__account__address__address_type:chararray,
      null as visit__account__address__city:chararray,
      null as visit__account__address__state:chararray,
      null as visit__account__address__zip_code:chararray,
      TOMAP('0',(chararray)geo_dma) as visit__account__details:map[chararray],
      null as visit__account__subscription:chararray,
      ((post_prop18 is not null)?post_prop18:post_evar18) as visit__account__subscription__service_level:chararray,
      null as visit__account__status:chararray,
      null as visit__account__enc_account_enc_billing_division:chararray,
      null as visit__account__enc_account_billing_id:chararray,
      null as visit__account__enc_account_enc_billing_division_id:chararray,
      null as visit__account__account_intest_division:boolean,
      null as visit__account__demographic_data:map[chararray],
      --visit.user
      ((post_prop2 is not null)?post_prop2:((post_evar2 is not null)?post_evar2:((post_prop19 is not null)?post_prop19:post_evar19))) as visit__user__enc_id:chararray,
      ((post_evar3 is not null)?post_evar3:post_prop3) as visit__user__role:chararray,
      null as visit__user__is_test_user:boolean,
      null as visit__user__is_whitelisted:boolean,
      TOMAP('post_campaign',(chararray)post_campaign) as visit__user__details:map[chararray],
      ((post_prop23 is not null)?post_prop23:post_evar23) as visit__user__status:chararray,
      --vsit.location
      null as visit__location__country_code:chararray,
      country as visit__location__country_name:chararray,
      geo_region as visit__location__region:chararray,
      null as visit__location__region_name:chararray,
      geo_city as visit__location__enc_city:chararray,
      geo_region as visit__location__state:chararray,
      geo_zip as visit__location__enc_zip_code:chararray,
      null as visit__location__enc_latitude:double,
      null as visit__location__enc_longitude :double,
      null as visit__location__metro_code:int,
      null as visit__location__area_code:int,
      post_t_time_info as visit__location__timezone:chararray,
      null as visit__location__status:chararray,
      TOBAG(TOTUPLE(null)) as visit__fips__fips_codes:bag{tuple(chararray)},
      null as visit__fips__fips_county:chararray,
      null as visit__fips__fips_county_section:chararray,
      null as visit__fips__fips_national:chararray,
      null as visit__fips__fips_state:chararray,
      --application
      null as application__drm_enabled:boolean,
      --application.error
      null as application__error__error_type:chararray,
      null as application__error__error_code:chararray,
      null as application__error__error_extras:map[chararray],
      null as application__error__error_message:chararray,
      --application.api
      null as application__api__response_code:chararray,
      null as application__api__service_result:chararray,
      post_prop27 as application__api__response_text:chararray,

      null as application__api__response_time_ms:long,
      null as application__api__host:chararray,
      null as application__api__path:chararray,
      null as application__api__api_name:chararray,
      null as application__api__http_verb:chararray,
      null as application__api__api_cached:boolean,
      null as application__api__query_parameters:chararray,
      null as application__api__trace_id:chararray,
      --message
      (chararray)(CASE post_page_event
        WHEN 0 THEN 'Page View'
        WHEN 100 THEN 'Custom Link'
        WHEN 101 THEN 'Download Link'
        WHEN 102 THEN 'Exit Link'
        ELSE (chararray)post_page_event
        END) as message__category:chararray, -- updated CASE ELSE to point to post_page_event instead of 'Other'
      raw_sbnet_calc::message_name as message__name:chararray,
      (int)raw_sbnet_calc::sequence_number as message__sequence_number:int,
      post_cust_hit_time_gmt as message__timestamp:long,
      null as message__triggered_by:chararray,
      --message.feature
      ((post_evar21 is not null)?post_evar21:post_prop21) as message__feature__name:chararray,
      null as message__feature__current_step:int,
      null as message__feature__number_of_steps:int,
      --state
      null as state__name:chararray,
      null as state__entry_timestamp:long,
      null as state__entry_video_position:int,
      null as state__current_video_position:int,
      --state.previous_state
      null as state__previous_state__name:chararray,
      null as state__previous_state__entry_timestamp:long,
      null as state__previous_state__entry_video_position:int,
      null as state__previous_state__exit_timestamp:long,
      null as state__previous_state__exit_video_position:int,
      --state.playback
      null as state__playback__playback_started_timestamp:long,
      null as state__playback__tune_time_ms:long,
      null as state__playback__uri_obtained_ms:int,
      (post_page_event_var2 == 'Play' ? post_cust_hit_time_gmt : null) as state__playback__playback_selected_timestamp:long,
      null as state__playback__buffering_time_ms:int,
      null as state__playback__segment_count:int,
      null as state__playback__segment_download_duration_ms:int,
      null as state__playback__segment_ip:chararray,
      null as state__playback__segment_location:chararray,
      --state.playback.heart_beat
      null as state__playback__heart_beat__clock_time_elapsed_sec:int,
      null as state__playback__heart_beat__content_elapsed_sec:int,
      --state.playback.bit_rate
      null as state__playback__bit_rate__current_bit_rate:double,
      null as state__playback__bit_rate__content_elapsed_at_current_bit_rate_sec:double,
      null as state__playback__bit_rate__previous_bit_rate:double,
      null as state__playback__bit_rate__content_elapsed_at_previous_bit_rate_sec:double,

      --state.playback.trick_play
      null as state__playback__trick_play__scrub_speed:chararray,
      null as state__playback__trick_play__scrub_type:chararray,
      null as state__playback__trick_play__scrub_start_position:int,
      null as state__playback__trick_play__scrub_end_position:int,
      --state.playback.segment_info
      null as state__playback__segment_info__segment_is_ad:boolean,
      null as state__playback__segment_info__query_parameters:chararray,
      null as state__playback__segment_info__sequence_number:int,
      null as state__playback__segment_info__segment_url:chararray,
      null as state__playback__segment_info__size_bytes:int,
      null as state__playback__segment_info__download_duration_ms:int,
      null as state__playback__segment_info__ip_address:chararray,
      --state.content
      null as state__content__purchase_id:chararray,
      null as state__content__content_class:chararray,
      null as state__content__content_format:chararray,
      null as state__content__rental_duration_hours:int,
      null as state__content__rental_expiration_date:long,
      --state.content.stream
      null as state__content__stream__content_uri:chararray,
      null as state__content__stream__playback_type:chararray,
      null as state__content__stream__streaming_format:chararray,
      null as state__content__stream__drm_type:chararray,
      null as state__content__stream__scrubbing_capability:chararray,
      null as state__content__stream__closed_captioning_capable:boolean,
      null as state__content__stream__sap_capable:boolean,
      null as state__content__stream__bookmark_position_sec:int,
      null as state__content__stream__entitled:boolean,
      null as state__content__stream__parentally_blocked:boolean,
      null as state__content__stream__playback_id:chararray,
      null as state__content__stream__price:chararray,
      null as state__content__stream__purchase_type:chararray,
      null as state__content__stream__start_timestamp:long,
      null as state__content__stream__end_timestamp:long,
      null as state__content__stream__recording_start_timestamp:long,
      null as state__content__stream__recording_stop_timestamp:long,
      null as state__content__stream__programmer_favorited:boolean,
      null as state__content__stream__programmer_call_sign:chararray,
      --state.content.identifiers
      null as state__content__identifiers__dvr_recording_id:chararray,
      null as state__content__identifiers__provider_asset_id:chararray,
      null as state__content__identifiers__platform_series_id:chararray,
      null as state__content__identifiers__platform_asset_id:chararray,
      null as state__content__identifiers__tms_series_id:chararray,
      null as state__content__identifiers__tms_program_id:chararray,
      null as state__content__identifiers__tms_guide_id:chararray,
      --state.content.details
      null as state__content__details__program_type:chararray,
      null as state__content__details__title:chararray,
      null as state__content__details__episode_number:chararray,
      null as state__content__details__season_number:chararray,
      null as state__content__details__available_date:long,
      null as state__content__details__service_closed_captioned:boolean,
      null as state__content__details__edition:chararray,
      null as state__content__details__runtime:long,
      null as state__content__details__actual_runtime:long,
      null as state__content__details__expiration_date:long,
      null as state__content__details__original_air_date:long,
      null as state__content__details__original_network_name:chararray,
      null as state__content__details__rating__rating:chararray,
      null as state__content__details__rating__rating_type:chararray,
      null as state__content__details__description:chararray,
      null as state__content__details__year:int,
      null as state__content__details__episode_title:chararray,
      TOBAG(TOTUPLE(null)) as state__content__details__content_format:bag{tuple(chararray)},
      TOBAG(TOTUPLE(null)) as state__content__details__actors:bag{tuple(chararray)},
      TOBAG(TOTUPLE(null)) as state__content__details__genres:bag{tuple(chararray)},
      TOBAG(TOTUPLE(null)) as state__content__details__asset_studios:bag{tuple(chararray)},

      --state.content.programmer.vod
      null as state__content__programmer__vod__provider_name:chararray,
      null as state__content__programmer__vod__product:chararray,
      --state.content.programmer.linear
      null as state__content__programmer__linear__channel_name:chararray,
      null as state__content__programmer__linear__network_name:chararray,
      null as state__content__programmer__linear__channel_category:chararray,
      null as state__content__programmer__linear__channel_number:chararray,
      null as state__content__programmer__linear__linear_channel_id:chararray,
      null as state__content__programmer__linear__linear_airing_time:long,

      --state.content.ad
      null as state__ad__id:chararray,
      null as state__ad__title:chararray,
      null as state__ad__number:int,
      null as state__ad__start_timestamp:long,
      null as state__ad__ad_duration_sec:int,
      null as state__ad__ad_elapsed_sec:int,
      null as state__ad__ad_stopped_reason:chararray,
      null as state__ad__ad_break_start_timestamp:long,
      null as state__ad__ad_break_duration_sec:int,
      null as state__ad__ad_break_start_position:int,

      null as state__ad__ad_break_number:int,
      null as state__ad__commodity_code:chararray,
      null as state__ad__vast:map[chararray],
      null as state__ad__campaign:chararray,
      null as state__ad__device_ad_id:chararray,

      --state.view.current_page
      ((pagename is not null)?pagename:((post_prop10 is not null)?post_prop10:((post_prop12 is not null)?post_prop12:post_evar12))) as state__view__current_page__page_name:chararray,
      ((channel is not null)?channel:((post_prop11 is not null)?post_prop11:((post_prop13 is not null)?post_prop13:post_evar13))) as state__view__current_page__section:chararray,
      ((post_prop4 is not null)?post_prop4:((post_prop15 is not null)?post_prop15:((post_prop14 is not null)?post_prop14:post_evar14))) as state__view__current_page__sub_section:chararray,
      post_page_url as state__view__current_page__page_id:chararray,
      null as state__view__current_page__page_load_time_ms:int,

      post_page_type as state__view__current_page__page_type:chararray,
      null as state__view__current_page__display_type:chararray,
      null as state__view__current_page__page_number:chararray,
      null as state__view__current_page__settings:map[chararray],
      --state.view.current_page.sort_and_filter
      null as state__view__current_page__sort_and_filter__applied_sorts:chararray,
      null as state__view__current_page__sort_and_filter__applied_filters:chararray,

      --state.view.current_page.elements
      ((post_prop9 is not null)?post_prop9:post_evar9) as state__view__current_page__elements__name:chararray,
      null as state__view__current_page__elements__number_of_items:int,
      null as state__view__current_page__elements__widgets_type:chararray,
      null as state__view__current_page__elements__parentally_blocked_items:int,
      null as state__view__current_page__elements__entitled_items:int,
      --state.view.previous_page
      ((post_prop5 is not null)?post_prop5:post_evar5) as state__view__previous_page__page_name:chararray,
      ((post_prop6 is not null)?post_prop6:post_evar6) as state__view__previous_page__section:chararray,
      ((post_prop7 is not null)?post_prop7:post_evar7) as state__view__previous_page__sub_section:chararray,
      post_referrer as state__view__previous_page__page_id:chararray,
      null as state__view__previous_page__page_viewed_time_sec:int,
      null as state__view__previous_page__page_load_time_ms:int,
      --state.view.modal
      null as state__view__modal__name:chararray,
      null as state__view__modal__text:chararray,
      null as state__view__modal__modal_type:chararray,
      null as state__view__modal__load_time_ms:int,
      --state.view.download
      null as state__download__content_window:long,
      null as state__download__quality:chararray,
      null as state__download__watched:boolean,
      null as state__download__duration_sec:int,
      --state.search
      null as state__search__number_of_search_results:int,
      null as state__search__selected_result_name:chararray,
      null as state__search__selected_result_facet:chararray,
      ((post_prop20 is not null)?post_prop20:post_evar20) as state__search__text:chararray,
      null as state__search__search_type:chararray,
      TOBAG(TOTUPLE(null)) as state__search__results:bag{tuple(chararray)},
      null as state__search__results_ms:int,
      null as state__search__query_id:chararray,
      null as state__search__search_id:chararray,
      --operation
      null as operation__toggle_state:boolean,
      null as operation__operation_type:chararray,
      null as operation__dvr_operation:chararray,
      null as operation__switch_screen_direction:chararray,
      null as operation__switch_screen_id:chararray,
      null as operation__success:boolean,
      --operation.selected
      null as operation__selected__index:int,
      null as operation__selected__view_element_name:chararray,
      --operation.user_entry
      null as operation__user_entry__enc_text:chararray,
      null as operation__user_entry__entry_type:chararray,
      null as operation__user_entry__pin_type:chararray,
      null as operation__user_entry__pin_change_type:chararray,
      null as operation__user_entry__operation_result:chararray;

-- DUMP sbnet_events $ENV;

STORE sbnet_events INTO '$TMP_db.sbnet_dev_events_no_part' USING org.apache.hive.hcatalog.pig.HCatStorer();
