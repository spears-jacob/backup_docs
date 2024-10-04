USE ${env:TMP_db};

SELECT '***** Creating ReprocessDateTable ******'
;

CREATE TABLE IF NOt EXISTS ${env:ReprocessDateTable} (
  run_date string)
;

INSERT OVERWRITE TABLE ${env:ReprocessDateTable} VALUES('${env:RUN_DATE}');

SELECT '***** ReprocessDateTable_daily created ******'
;


USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_specmobile_events
(
domain string,
customer string,
received__timestamp bigint,
unfinished_created_timestamp bigint,
finished_created_timestamp bigint,
visit__visit_id string,
visit__visit_start_timestamp bigint,
visit__device__device_form_factor string,
visit__device__device_type string,
visit__device__model string,
visit__device__operating_system string,
visit__device__uuid string,
visit__device__available_storage string,
visit__login__failed_attempts int,
visit__login__login_duration_ms int,
visit__isp__ip_address string,
visit__isp__isp string,
visit__connection__connection_type string,
visit__connection__network_cell_carrier string,
visit__connection__network_cell_network_type string,
visit__account__account_number string,
visit__location__zip_code string,
visit__location__latitude string,
visit__location__longitude string,
visit__location__time_zone string,
visit__application_details__application_name string,
visit__application_details__platform_type string,
visit__application_details__application_type string,
visit__application_details__app_version string,
state__view__modal__name string,
state__view__modal__text string,
state__view__modal__modal_type string,
application__error__error_type string,
application__error__error_code string,
application__api__response_code string,
application__api__service_result string,
application__api__response_time_ms int,
application__api__host string,
application__api__path string,
application__api__api_name string,
application__api__api_category string,
message__category string,
message__name string,
message__sequence_number int,
message__timestamp bigint,
message__triggered_by string,
state__name string,
state__entry_timestamp bigint,
state__previous_state__name string,
state__content__status string,
state__view__current_page__page_name string,
state__view__current_page__app_section string,
state__view__current_page__render_details__fully_rendered_ms bigint,
state__view__current_page__render_details__view_rendered_status string,
state__view__current_page__page_sequence_number int,
state__view__current_page__elements__standardized_name string,
state__view__previous_page__page_name string,
state__view__previous_page__app_section string,
state__view__previous_page__page_viewed_time_ms int,
operation__operation_type string,
operation__success boolean,
operation__user_entry__entry_type string,
state__view__current_page__render_details__is_lazy_load boolean,
visit__device__uuid_aes_256 string,
visit__account__account_number_aes_256 string,
visit__login__logged_in boolean,
visit__device__manufacturer string,
message__event_case_id string,
visit__device__used_storage string
)
PARTITIONED BY
(
    PARTITION_DATE_ET string,
    PARTITION_DATE_HOUR_ET string
)
TBLPROPERTIES ('retention_policy'='Aggregate - With PII (3 Years)')
;

CREATE VIEW IF NOT EXISTS asp_v_specmobile_events AS
SELECT * from prod.asp_specmobile_events;


SELECT '***** View & Table creation complete ******'
;
