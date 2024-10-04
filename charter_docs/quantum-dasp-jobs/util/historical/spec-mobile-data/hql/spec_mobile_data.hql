USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.execution.engine=tez;
SET hive.tez.container.size=8240;
SET hive.tez.java.opts=-Xmx8192m;
SET hive.auto.convert.join=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;

SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/jdatehour-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_datehour AS 'Epoch_Datehour';

INSERT overwrite TABLE ${env:DASP_db}.asp_specmobile_events PARTITION(PARTITION_DATE_ET,PARTITION_DATE_HOUR_ET)
Select
  domain,
  customer,
  received__timestamp,
  unfinished_created_timestamp,
  finished_created_timestamp,
  visit__visit_id,
  visit__visit_start_timestamp,
  visit__device__device_form_factor,
  visit__device__device_type,
  visit__device__model,
  visit__device__operating_system,
  visit__device__uuid,
  visit__device__available_storage,
  visit__login__failed_attempts,
  visit__login__login_duration_ms,
  visit__isp__enc_ip_address AS visit__isp__ip_address,
  visit__isp__isp,
  visit__connection__connection_type,
  visit__connection__network_cell_carrier,
  visit__connection__network_cell_network_type,
  visit__account__account_number,
  visit__location__enc_zip_code AS visit__location__zip_code,
  visit__location__latitude,
  visit__location__longitude,
  visit__location__time_zone,
  visit__application_details__application_name,
  visit__application_details__platform_type,
  visit__application_details__application_type,
  visit__application_details__app_version,
  state__view__modal__name,
  state__view__modal__text,
  state__view__modal__modal_type,
  application__error__error_type,
  application__error__error_code,
  application__api__response_code,
  application__api__service_result,
  application__api__response_time_ms,
  application__api__host,
  application__api__path,
  application__api__api_name,
  application__api__api_category,
  message__category,
  message__name,
  message__sequence_number,
  message__timestamp,
  message__triggered_by,
  state__name,
  state__entry_timestamp,
  state__previous_state__name,
  state__content__status,
  state__view__current_page__page_name,
  state__view__current_page__app_section,
  state__view__current_page__render_details__fully_rendered_ms,
  state__view__current_page__render_details__view_rendered_status,
  state__view__current_page__page_sequence_number,
  state__view__current_page__elements__standardized_name,
  state__view__previous_page__page_name,
  state__view__previous_page__app_section,
  state__view__previous_page__page_viewed_time_ms,
  operation__operation_type,
  operation__success,
  operation__user_entry__entry_type,
  state__view__current_page__render_details__is_lazy_load,
  visit__device__enc_uuid AS visit__device__uuid_aes_256,
  visit__account__enc_account_number AS visit__account__account_number_aes_256,
  visit__login__logged_in,
  visit__device__manufacturer,
  message__event_case_id,
  visit__device__used_storage,
  epoch_converter(received__timestamp,'America/New_York') partition_date_et,
  epoch_datehour (received__timestamp,'America/New_York') partition_date_hour_et
from  ${env:ENVIRONMENT}.core_quantum_events_sspp
WHERE partition_date_hour_utc >= '${hiveconf:START_DATE_TZ}'
  AND partition_date_hour_utc <  '${hiveconf:END_DATE_TZ}'
  AND visit__application_details__application_name = 'SpecMobile'
  and visit__account__enc_account_number IS NOT NULL
;
