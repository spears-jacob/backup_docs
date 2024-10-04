USE ${env:DASP_db};

set hive.exec.dynamic.partition.mode=nonstrict;

--updates table with data from previous seven days (relative to RUN_DATE)

 MSCK REPAIR TABLE cs_calls_with_prior_visits;
 MSCK REPAIR TABLE quantum_m2dot0_metric_agg;

insert overwrite table m2dot0_metric_agg
partition (partition_date_utc)
USE ${env:DASP_db};

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=2048000000;
set hive.merge.size.per.task=2048000000;

ADD JAR ${env:ARTIFACTS_PATH}/jars/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

INSERT OVERWRITE TABLE asp_m2dot0_metric_agg PARTITION (partition_date_utc)

select
ma.visit__application_details__application_name as visit__application_details__application_name,
ma.visit__application_details__application_type as visit__application_details__application_type,
ma.visit__application_details__app_version as visit__application_details__app_version,
ma.CUSTOM_visit__application_details__app_version as CUSTOM_visit__application_details__app_version,
ma.agg_CUSTOM_visit__account__details__service_subscriptions as agg_CUSTOM_visit__account__details__service_subscriptions,
ma.agg_CUSTOM_visit__account__details__service_subscriptions_other as agg_CUSTOM_visit__account__details__service_subscriptions_other,
ma.agg_CUSTOM_visit__account__details__service_subscriptions_core as agg_CUSTOM_visit__account__details__service_subscriptions_core,
ma.agg_CUSTOM_visit__account__details__service_subscriptions_mobile as agg_CUSTOM_visit__account__details__service_subscriptions_mobile,
ma.agg_CUSTOM_visit__account__configuration_factors as agg_CUSTOM_visit__account__configuration_factors,
ma.agg_visit__account__configuration_factors as agg_visit__account__configuration_factors,
ma.agg_custom_customer_group as agg_custom_customer_group,
ma.agg_visit__account__details__mso as agg_visit__account__details__mso,
ma.portals_unique_acct_key as portals_unique_acct_key,
ma.visit__account__enc_mobile_account_number as visit__account__enc_mobile_account_number,
ma.visit_id as visit_id,
ma.visit__visit_start_timestamp as visit__visit_start_timestamp,
ma.device_id as device_id,

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
