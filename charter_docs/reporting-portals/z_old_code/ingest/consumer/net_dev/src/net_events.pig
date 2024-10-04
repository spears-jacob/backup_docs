SET job.priority HIGH;
SET output.compression.enabled true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
SET pig.exec.mapPartAgg true; -- for optimizing the group by statements
SET pig.cachedbag.memusage 0.35; -- increased the memory usage of the pig script by 15%. Previously it was 20% and now it is 35%

raw_net_de = LOAD '$TMP.net_dev_denorm' USING org.apache.hive.hcatalog.pig.HCatLoader();
raw_net_calc = LOAD '$TMP.net_dev_calc' USING org.apache.hive.hcatalog.pig.HCatLoader();

raw_net = JOIN raw_net_de BY unique_id LEFT OUTER, raw_net_calc BY unique_id;

raw_net = FILTER raw_net BY post_cust_hit_time_gmt IS NOT NULL;

net_events = foreach raw_net generate
      'resi' as domain:chararray,
      'Charter Communications' as customer:chararray,
      null as received_timestamp:long,
      null as unfinished_created_timestamp:long,
      null as finished_created_timestamp:long,
      visit_id as visit__visit_id:chararray, --visit.visit_id
      previous_visit_id as visit__previous_visit_id:chararray, --visit.previous_visit_id
      raw_net_calc::visit_start_timestamp as visit__visit_start_timestamp:long, --visit.visit_start_timestamp
      null as visit__visit_startup_time:int, --visit.visit_startup_time
      ((post_prop11 is not null)?post_prop11:post_evar11) as visit__referrer_link:chararray, --visit.referrer_link
      post_event_list as visit__device__type:chararray, --get all events here 2017-12-19
      null as visit__device__model:chararray, --visit.device.model
      os as visit__device__operating_system:chararray,--visit.device.operating_system
      (hit_source!=5?(hit_source!=7?(hit_source!=8?(hit_source!=9?CONCAT((chararray)post_visid_high,(chararray)post_visid_low):''):''):''):'') as visit__device__enc_uuid:chararray, --visit.device.uuid
      null as visit__device__available_storage:chararray, --visit.device.available_storage
      browser as visit__device__browser__name:chararray, --visit.device.browser.name
      user_agent as visit__device__browser__version:chararray, --pull user_agent straight across 2017-12-22
      null as visit__device__linked_device__dvr_capable:boolean, --visit.device.linked_device.dvr_capable
      null as visit__device__linked_device__name:chararray, --visit.device.linked_device.name
      null as visit__device__enc_linked_device__id:chararray, --visit.device.linked_device.id
      --visit.login
      (int)((post_page_event_var2 == 'Sign In' and post_evar26 matches '*success*')? 1:0) as visit__login__failed_attempts:int,--visit.login.failed_attempts
      (double)raw_net_calc::visit_login_duration as visit__login__login_duration:double,  --visit.login.login_duration
      (((post_page_event_var2 == 'Sign In' AND post_prop26 == 'success') OR (post_prop27 matches '*"BehindTheModem":true*') )? post_cust_hit_time_gmt:null) as visit__login__login_completed_timestamp:long, --visit.login.login_completed_timestamp
      --visit.video_zone
      null as visit__video_zone__details:map[chararray],--visit.video_zone.details
      --visit.isp
      ip as visit__isp__enc_ip_address:chararray, --visit.isp.ip_address
      null as visit__isp__location_id:chararray,--visit.isp.location_id
      null as visit__isp__organization:chararray, --visit.isp.organization
      domain as visit__isp__isp:chararray, --visit.isp.isp
      null as visit__isp__status:chararray, --visit.isp.status
      --visit.connection
      connection_type as visit__connection__type:chararray, --visit.connection.type
      ((post_prop4 is not null)?post_prop4:post_evar4) as visit__connection__network_status:chararray, --visit.connection.network_status
      null as visit__connection__online:boolean, --visit.connection.online
      --visit.connection.speed_test
      null as visit__connection__speed_test__output:chararray, --visit.connection.speed_test.output
      null as visit__connection__speed_test__start_time:long, --visit.connection.speed_test.start_time
      null as visit__connection__speed_test__end_time:long,--visit.connection.speed_test.end_time
      --visit.account
      null as visit__account__id:chararray, --visit.account.id
      ((post_prop1 is not null)?post_prop1:post_evar1) as visit__account__enc_account_number:chararray,  --visit.account.account_number
      --visit.account.address
      null as visit__account__address__address_type:chararray,  --visit.account.address.address_type
      null as visit__account__address__city:chararray, --visit.account.address.city
      null as visit__account__address__state:chararray, --visit.account.address.state
      post_evar58 as visit__account__address__zip_code:chararray,--visit.account.address.zip_code
      TOMAP('0',(chararray)geo_dma) as visit__account__details:map[chararray], --visit.account.details
      --visit.account.subscription
      ((post_prop5 is not null)?post_prop5:post_evar5) as visit__account__subscription__service_level:chararray, --visit.account.subscription.service_level
      null as visit__account__status:chararray, --visit.account.status
      --visit.user
      ((post_prop2 is not null)?post_prop2:post_evar2) as visit__user__enc_id:chararray, --visit.user.id
      ((post_prop6 is not null)?post_prop6:post_evar6) as visit__user__role:chararray, --visit.user.role
      null as visit__user__is_test_user:boolean, --visit.user.is_test_user
      TOMAP('post_campaign',(chararray)post_campaign) as visit__user__details:map[chararray], --visit.user.details
      ((post_prop28 is not null)?post_prop28:post_evar28) as visit__user__status:chararray, --visit.user.status
      --visit.location
      null as visit__location__country_code:chararray, --visit.location.country_code
      country as visit__location__country_name:chararray, --visit.location.country_name
      geo_region as visit__location__region:chararray,  --visit.location.region
      null as visit__location__region_name:chararray, --visit.location.region_name
      geo_city as visit__location__enc_city:chararray, --visit.location.city
      geo_region as visit__location__state:chararray,  --visit.location.state
      geo_zip as visit__location__enc_zip_code:chararray, --visit.location.zip_code
      null as visit__location__enc_latitude:chararray, --visit.location.latitude
      null as visit__location__enc_longitude:chararray, --visit.location.longitude
      null as visit__location__metro_code:chararray, --visit.location.metro_code
      post_t_time_info as visit__location__timezone:chararray, --visit.location.timezone
      null as visit__location__status:chararray, --visit.location.status
      null as visit__app_version:chararray, --visit.app_version
      null as visit__library_version:chararray, --visit.library_version
      null as visit__app_name:chararray, --visit.app_name
      null as visit__environment:chararray, --visit.environment
      resolution as visit__resolution:chararray, --visit.resolution
      null as visit__settings:MAP[chararray], --visit.settings
      --application
      --application.error
      null as application__error__type:chararray,  --application.error.type
      null as application__error__code:chararray, --application.error.code
      null as application__error__details:chararray, --application.error.details
      --application.api
      null as application__api__response_code:chararray, --application.api.response_code
      post_prop27 as application__api__response_text:chararray, --application.api.exception
      null as application__api__timeout:boolean, --application.api.timeout
      null as application__api__response_time_in_ms:long, --application.api.response_time_in_ms
      null as application__api__host:chararray, --application.api.host
      null as application__api__api_name:chararray, --application.api.api_name
      null as application__api__http_verb:chararray, --application.api.http_verb
      null as application__api__query_parameters:chararray, --application.api.query_parameters
      (chararray)(CASE post_page_event
        WHEN 0 THEN 'Page View'
        WHEN 100 THEN 'Custom Link'
        WHEN 101 THEN 'Download Link'
        WHEN 102 THEN 'Exit Link'
        ELSE (chararray)post_page_event
        END) as message__category:chararray, -- updated CASE ELSE to point to post_page_event instead of 'Other'
      raw_net_calc::message_name as message__name:chararray, --message.name
      (int)raw_net_calc::sequence_number as message__sequence_number:int, --message.sequence_number
      post_cust_hit_time_gmt as message__timestamp:long, --message.timestamp
      null as message__initiated_by:chararray, --message.initiated_by
       --message.feature
      null as message__feature__name:chararray, --message.feature.name
      null as message__feature__current_step:int, --message.feature.current_step
      null as message__feature__number_of_steps:int,--message.feature.number_of_steps
      --state
      null as state__name:chararray, --state.name
      null as state__entry_timestamp:long, --state.entry_timestamp
      null as state__entry_content_position:int, --state.entry_content_position
      null as state__current_content_position:int, --state.current_content_position
      --state.previous_state
      null as state__previous_state__name:chararray, --state.previous_state.name
      null as state__previous_state__entry_timestamp:long, --state.previous_state.entry_timestamp
      null as state__previous_state__entry_content_position:int, --state.previous_state.entry_content_position
      null as state__previous_state__exit_timestamp:long, --state.previous_state.exit_timestamp
      null as state__previous_state__exit_content_position:int,--state.previous_state.exit_content_position
      --state.playback
      null as state__playback__playback_started_timestamp:long, --state.playback.playback_started_timestamp
      (post_page_event_var2 == 'Play' ? post_cust_hit_time_gmt : null) as  state__playback__tune_timestamp:long,  --state.playback.tune_timestamp
      --state.playback.heartbeat
      null as state__playback__heart_beat__time_elapsed_in_seconds:int, --state.playback.heart_beat.time_elapsed_in_seconds
      null as state__playback__heart_beat__content_elapsed_in_seconds:int, --state.playback.heart_beat.content_elapsedin_seconds
      --state.playback.bitrate
      null as state__playback__bit_rate__current_bit_rate:double, --state.playback.bit_rate.current_bit_rate
      null as state__playback__bit_rate__content_elapsed_at_current_bit_rate:double, --state.playback.bit_rate.content_elapsed_at_current_bit_rate
      null as state__playback__bit_rate__previous_bit_rate:double, --state.playback.bit_rate.previous_bit_rate
      null as state__playback__bit_rate__content_elapsed_at_previous_bit_rate:double, --state.playback.bit_rate.content_elapsed_at_previous_bit_rate
      --state.playback.trick_play
      null as state__playback__trick_play__scrub_speed:chararray, --state.playback.trick_play.scrub_speed
      null as state__playback__trick_play__scrub_type:chararray, --state.playback.trick_play.scrub_type
      null as state__playback__trick_play__scrub_start_position:int, --state.playback.trick_play.scrub_start_position
      null as state__playback__trick_play__scrub_end_position:int, --state.playback.trick_play.scrub_end_position
      --state.content
      --state.content.stream
      null as state__content__stream__content_uri:chararray, --state.content.stream.content_uri
      post_prop33 as state__content__stream__type:chararray, --state.content.stream.type
      post_prop37 as state__content__stream__content_format:chararray, --state.content.stream.content_format
      null as state__content__stream__drm_type:chararray, --state.content.stream.drm_type
      null as state__content__stream__scrubbing_capability:chararray, --state.content.stream.scrubbing_capability
      null as state__content__stream__closed_captioning_capable:boolean, --state.content.stream.closed_captioning_capable
      null as state__content__stream__bookmark_position:int, --state.content.stream.bookmark_position
      null as state__content__stream__entitled:boolean, --state.content.stream.entitled
      null as state__content__stream__parentally_blocked:boolean, --state.content.stream.parentally_blocked
      null as state__content__stream__favorited:boolean, --state.content.stream.favorited
      (post_page_event_var2 == 'Play' ? (CONCAT((chararray)visit_id,(chararray)post_cust_hit_time_gmt)) : null) as state__content__stream__view_id:chararray, --state.content.stream.view_id
      null as state__content__stream__price:chararray, --state.content.stream.price
      null as state__content__stream__license_type:chararray, --state.content.stream.license_type
      null as state__content__stream__start_timestamp:long, --state.content.stream.start_timestamp
      null as state__content__stream__end_timestamp:long,--state.content.stream.end_timestamp
      --state.content.identifiers
      ((post_prop17 is not null)?post_prop17:post_evar17) as state__content__identifiers__id:chararray, --state.content.identifiers.id
      'TMS' as state__content__identifiers__id_type:chararray, --state.content.identifiers.id_type
      null as state__content__identifiers__series_id_type:chararray, --state.content.identifiers.series_id_type
      null as state__content__identifiers__series_id:chararray, --state.content.identifiers.series_id
      null as state__content__identifiers__recording_timestamp:long, --state.content.identifiers.recording_timestamp
      --state.content.details
      ((post_prop20 is not null)?post_prop20:post_evar20) as state__content__details__type:chararray, --state.content.details.type
      ((post_prop18 is not null)?post_prop18:post_evar18) as state__content__details__title:chararray,  --state.content.details.title
      post_prop31 as state__content__details__episode_number:chararray, --state.content.details.episode_number
      post_prop32 as state__content__details__season_number:chararray, --state.content.details.season_number
      null as state__content__details__available_date:long, --state.content.details.available_date
      null as state__content__details__closed_captioned:boolean, --state.content.details.closed_captioned
      null as state__content__details__long_form:boolean,  --state.content.details.long_form
      (long)post_prop38 as state__content__details__runtime:long, --state.content.details.runtime
      null as state__content__details__expiration_date:long, --state.content.details.expiration_date
      null as state__content__details__original_air_date:long, --state.content.details.original_air_date
      null as state__content__details__original_network_name:chararray, --state.content.details.original_network_name
      post_prop39 as state__content__details__rating:chararray, --state.content.details.rating
      null as state__content__details__description:chararray, --state.content.details.description
      null as state__content__details__year:chararray, --state.content.details.year
      post_prop30 as state__content__details__episode_title:chararray, --state.content.details.episode_title
      null as state__content__details__hd:boolean,  --state.content.details.hd
      TOBAG(TOTUPLE(((post_prop21 is not null)?post_prop21:post_evar21))) as state__content__details__actors:bag{tuple(chararray)},  --state.content.details.actors
      TOBAG(TOTUPLE(((post_prop19 is not null)?post_prop19:post_evar19))) as state__content__details__genres:bag{tuple(chararray)}, --state.content.details.genres
      --state.content.programmer
      null as state__content__programmer__callsign:chararray, --state.content.programmer.callsign
      ((post_prop34 is not null)?post_prop34:((post_prop12 is not null)?post_prop12:post_evar12)) as state__content__programmer__id:chararray, --state.content.programmer.id
      --state.content.programmer.vod
      null as state__content__programmer__vod__provider_id:chararray, --state.content.programmer.vod.provider_id
      null as state__content__programmer__vod__provider_name:chararray, --state.content.programmer.vod.provider_name
      null as state__content__programmer__vod__product:chararray, --state.content.programmer.vod.product
      --state.content.programmer.linear
      ((post_prop12 is not null)?post_prop12:post_evar12) as state__content__programmer__linear__channel_name:chararray,
      ((post_prop14 is not null)?post_prop14:post_evar14) as state__content__programmer__linear__network_name:chararray,
      ((post_prop15 is not null)?post_prop12:post_evar15) as state__content__programmer__linear__channel_category:chararray,
      ((post_prop13 is not null)?post_prop13:post_evar13) as state__content__programmer__linear__channel_number:chararray,
      --state.ad
      null as state__ad__id:chararray, --state.ad.id
      post_prop40 as state__ad__title:chararray, --state.ad.title
      (int)post_prop42 as state__ad__number:int, --state.ad.number
      null as state__ad__ad_start_timestamp:long, --state.ad.ad_start_timestamp
      null as state__ad__break_start_timestamp:long, --state.ad.break_start_timestamp
      null as state__ad__break_start_position:int, --state.ad.break_start_position
      null as state__ad__break_length_seconds:int, --state.ad.break_length_seconds
      null as state__ad__break_number:int, --state.ad.break_number
      null as state__ad__commodity_code:chararray, --state.ad.commodity_code
      null as state__ad__vast:MAP[chararray], --state.ad.vast
      null as state__ad__campaign:chararray, --state.ad.campaign
      --state.view
      --state.view.current_page
      ((pagename is not null)?pagename:((post_prop22 is not null)?post_prop22:post_evar22)) as state__view__current_page__name:chararray, --state.view.current_page.name
      ((channel is not null)?channel:((post_prop23 is not null)?post_prop23:post_prop9)) as state__view__current_page__section:chararray, --state.view.current_page.section
      ((post_prop24 is not null)?post_prop24:((post_prop7 is not null)?post_prop7:post_evar7)) as state__view__current_page__sub_section:chararray, --state.view.current_page.sub_section
      ((post_prop48 is not null)? post_prop48: post_page_url) as state__view__current_page__page_id:chararray,  --state.view.current_page.page_id
      (int)post_prop47 as state__view__current_page__page_load_time:int, --state.view.current_page.page_load_time
      post_page_type as state__view__current_page__type:chararray,  --state.view.current_page.type
      null as state__view__current_page__display_type:chararray, --state.view.current_page.display_type
      --state.view.current_page.sort_and_filter
      null as state__view__current_page__sort_and_filter__applied_sorts:chararray, --state.view.current_page.applied_sorts
      null as state__view__current_page__sort_and_filter__applied_filters:chararray, --state.view.current_page.applied_filters
      null as state__view__current_page__search_text:chararray, --state.view.current_page.search_text
      (chararray)visit_page_num as state__view__current_page__page_number:chararray, --state.view.current_page.page_number
      --state.view.current_page.elements
      post_prop25 as state__view__current_page__elements__name:chararray, --state.view.current_page.elements.avro
      null as state__view__current_page__elements__number_of_items:int, --state.view.current_page.elements.number_of_items
      null as state__view__current_page__elements__type:chararray, --state.view.current_page.elements.type --marking as n/a
      null as state__view__current_page__elements__parentally_blocked_items:int, --state.view.current_page.elements.parentally_blocked_items
      null as state__view__current_page__elements__entitled_items:int, --state.view.current_page.elements.entitled_items
      TOMAP('VideoPlayerID',(chararray)post_prop43, 'MVPD',(chararray)post_prop44,'VideoEmbeddedHost',(chararray)post_prop46) as state__view__current_page__settings:MAP[chararray],--state.view.current_page.settings
      --state.view.previous_page
      ((post_prop8 is not null)?post_prop8:post_evar8) as state__view__previous_page__name:chararray, --state.view.previous_page.name
      ((post_prop10 is not null)?post_prop10:post_evar10) as state__view__previous_page__section:chararray, --state.view.previous_page.section
      null as state__view__previous_page__sub_section:chararray, --state.view.previous_page.sub_section  as calculated value
      post_referrer as state__view__previous_page__page_id:chararray, --state.view.previous_page.page_id
      null as state__view__previous_page__page_load_time:int, --state.view.previous_page.page_load_time
      --state.view.modal
      null as state__view__modal__name:chararray, --state.view.modal.name
      null as state__view__modal__text:chararray, --state.view.modal.text
      null as state__view__modal__type:chararray, --state.view.modal.type
      null as state__view__modal__load_time:int, --state.view.modal.load_time
      --state.download
      null as state__download__content_window:long, --state.download.content_window
      null as state__download__quality:chararray, --state.download.quality
      null as state__download__watched:boolean, --state.download.watched
      --operation
      null as operation__toggle_state:boolean, --operation.toggle_state
      post_prop26 as operation__type:chararray, --operation.type
      null as operation__success:boolean, --operation.success
      --operation.selected
      null as operation__selected__index:int, --operation.selected.index
      null as operation__selected__view_element_name:chararray, --operation.selected.view_element_name
      --operation.user_entry
      null as operation__user_entry__enc_text:chararray, --operation.user_entry.text
      post_prop23 as operation__user_entry__type:chararray; --operation.user_entry.type

-- DUMP net_events $ENV;

STORE net_events INTO '$TMP.net_dev_events_no_part' USING org.apache.hive.hcatalog.pig.HCatStorer();
