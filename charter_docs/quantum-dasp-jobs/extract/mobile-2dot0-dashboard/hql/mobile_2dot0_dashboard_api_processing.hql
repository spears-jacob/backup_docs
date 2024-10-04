USE ${env:DASP_db};

set hive.exec.dynamic.partition.mode=nonstrict;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;
SET hive.exec.max.dynamic.partitions.pernode=800;
SET hive.exec.max.dynamic.partitions=8000;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;

--------------------------------------------------------------------------------
----------------------- ***** API Data Processing ***** ------------------------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_m2dot0_apiRaw
PARTITION (partition_date_utc)
select
visit__visit_id
,visit__application_details__application_name
,application__api__gql__operation_name
,application__api__service_result
,application__api__response_time_ms
,application__api__api_name
,message__sequence_number
,partition_date_utc
from `${env:PCQE}`
where (partition_date_utc >= '${env:STARTDATE_5day}' AND partition_date_utc < '${env:END_DATE}')
   and visit__application_details__application_name in ('SpecNet','MySpectrum')
   and message__name = 'apiCall'
   and application__api__api_name = 'v1Graph'
;

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_m2dot0_apiJoin
PARTITION (partition_date_utc)
SELECT
visit__account__enc_account_number
,AGG_visit__account__details__mso
,visit__account__enc_mobile_account_number
,v.visit__application_details__application_name
,visit__application_details__application_type
,visit__application_details__app_version
,AGG_visit__account__configuration_factors
,AGG_CUSTOM_visit__account__details__service_subscriptions
,AGG_CUSTOM_customer_group
,v.visit__visit_id
,application__api__gql__operation_name
,application__api__service_result
,cast(application__api__response_time_ms as bigint) as application__api__response_time_ms
,application__api__api_name
,message__sequence_number
,date_add(cast(v.partition_date_utc as date),7) AS partition_date_utc_week1
,date_add(cast(v.partition_date_utc as date),28) AS partition_date_utc_week4
--,date_add('day',7,cast(v.partition_date_utc as date)) as partition_date_utc_week1
--,date_add('day',28,cast(v.partition_date_utc as date)) as partition_date_utc_week4
,cast(v.partition_date_utc as date) as partition_date_utc
from ${env:DASP_db}.asp_m2dot0_visitsRanked v
--FROM ${env:TMP_db}.visits_ranked_${env:CLUSTER} v
inner join stg_dasp.asp_m2dot0_apiRaw a on v.visit__visit_id = a.visit__visit_id
  and v.visit__application_details__application_name = a.visit__application_details__application_name
  and v.partition_date_utc = a.partition_date_utc
where (v.partition_date_utc >= '${env:STARTDATE_5day}' AND v.partition_date_utc < '${env:END_DATE}')
  and (a.partition_date_utc >= '${env:STARTDATE_5day}' AND a.partition_date_utc < '${env:END_DATE}')
;

--------------------------------------------------------------------------------
------------------------- ***** API Aggregation ***** --------------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.asp_m2dot0_apiagg_tmp${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.asp_m2dot0_apiagg_tmp${env:CLUSTER} AS
select
visit__application_details__application_name
,AGG_visit__account__configuration_factors
,application__api__gql__operation_name
,cast(floor(avg(cast(application__api__response_time_ms as bigint))) as bigint) as response_time_ms_mean
,floor(PERCENTILE_APPROX(cast(application__api__response_time_ms as bigint),0.5)) as response_time_ms_median
,floor(PERCENTILE_APPROX(cast(application__api__response_time_ms as bigint),0.75)) as response_time_ms_75
,floor(PERCENTILE_APPROX(cast(application__api__response_time_ms as bigint),0.90)) as response_time_ms_90
,floor(PERCENTILE_APPROX(cast(application__api__response_time_ms as bigint),0.95)) as response_time_ms_95
,floor(PERCENTILE_APPROX(cast(application__api__response_time_ms as bigint),0.99)) as response_time_ms_99
,count(distinct visit__visit_id) as num_visits
,count(1) as num_events
,sum(if(application__api__service_result='success',1,0)) as num_success
,sum(if(application__api__service_result='failure',1,0)) as num_failure
,sum(if(application__api__service_result='partialFailure',1,0)) as num_partial_failure
,partition_date_utc_week1
,partition_date_utc_week4
,partition_date_utc
from ${env:DASP_db}.asp_m2dot0_apiJoin
where (partition_date_utc >= '${env:STARTDATE_5day}' AND partition_date_utc < '${env:END_DATE}')
group by
visit__application_details__application_name
,AGG_visit__account__configuration_factors
,application__api__gql__operation_name
,partition_date_utc_week1
,partition_date_utc_week4
,partition_date_utc
;

INSERT INTO ${env:TMP_db}.asp_m2dot0_apiagg_tmp${env:CLUSTER}
select
visit__application_details__application_name
,'Either' as AGG_visit__account__configuration_factors
,application__api__gql__operation_name
,cast(floor(avg(cast(application__api__response_time_ms as bigint))) as bigint) as response_time_ms_mean
,floor(PERCENTILE_APPROX(cast(application__api__response_time_ms as bigint),0.5)) as response_time_ms_median
,floor(PERCENTILE_APPROX(cast(application__api__response_time_ms as bigint),0.75)) as response_time_ms_75
,floor(PERCENTILE_APPROX(cast(application__api__response_time_ms as bigint),0.90)) as response_time_ms_90
,floor(PERCENTILE_APPROX(cast(application__api__response_time_ms as bigint),0.95)) as response_time_ms_95
,floor(PERCENTILE_APPROX(cast(application__api__response_time_ms as bigint),0.99)) as response_time_ms_99
,count(distinct visit__visit_id) as num_visits
,count(1) as num_events
,sum(if(application__api__service_result='success',1,0)) as num_success
,sum(if(application__api__service_result='failure',1,0)) as num_failure
,sum(if(application__api__service_result='partialFailure',1,0)) as num_partial_failure
,partition_date_utc_week1
,partition_date_utc_week4
,partition_date_utc
from ${env:DASP_db}.asp_m2dot0_apiJoin
where (partition_date_utc >= '${env:STARTDATE_5day}' AND partition_date_utc < '${env:END_DATE}')
group by
visit__application_details__application_name
,application__api__gql__operation_name
,partition_date_utc_week1
,partition_date_utc_week4
,partition_date_utc
;

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_m2dot0_apiAgg
PARTITION (partition_date_utc)
SELECT
visit__application_details__application_name
,AGG_visit__account__configuration_factors
,application__api__gql__operation_name
,response_time_ms_mean
,response_time_ms_median
,response_time_ms_75
,response_time_ms_90
,response_time_ms_95
,response_time_ms_99
,num_visits
,num_events
,num_success
,num_failure
,num_partial_failure
,partition_date_utc_week1
,partition_date_utc_week4
,partition_date_utc
FROM ${env:TMP_db}.asp_m2dot0_apiagg_tmp${env:CLUSTER}
;

--------------------------------------------------------------------------------
----------------------- ***** API Final Processing ***** -----------------------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_m2dot0_apiFinal
PARTITION (partition_date_utc)
select
x.visit__application_details__application_name
,x.AGG_visit__account__configuration_factors
,x.application__api__gql__operation_name
,x.response_time_ms_mean
,x.response_time_ms_median
,x.response_time_ms_75
,x.response_time_ms_90
,x.response_time_ms_95
,x.response_time_ms_99
,x.num_visits
,x.num_events
,x.num_success
,x.num_failure
,x.num_partial_failure
,week1.response_time_ms_mean as response_time_ms_mean_week1
,week1.response_time_ms_median as response_time_ms_median_week1
,week1.response_time_ms_75 as response_time_ms_75_week1
,week1.response_time_ms_90 as response_time_ms_90_week1
,week1.response_time_ms_95 as response_time_ms_95_week1
,week1.response_time_ms_99 as response_time_ms_99_week1
,week1.num_visits as num_visits_week1
,week1.num_events as num_events_week1
,week1.num_success as num_success_week1
,week1.num_failure as num_failure_week1
,week1.num_partial_failure as num_partial_failure_week1
,week1.partition_date_utc as partition_date_utc_week1
,week4.response_time_ms_mean as response_time_ms_mean_week4
,week4.response_time_ms_median as response_time_ms_median_week4
,week4.response_time_ms_75 as response_time_ms_75_week4
,week4.response_time_ms_90 as response_time_ms_90_week4
,week4.response_time_ms_95 as response_time_ms_95_week4
,week4.response_time_ms_99 as response_time_ms_99_week4
,week4.num_visits as num_visits_week4
,week4.num_events as num_events_week4
,week4.num_success as num_success_week4
,week4.num_failure as num_failure_week4
,week4.num_partial_failure as num_partial_failure_week4
,week4.partition_date_utc as partition_date_utc_week4
,x.partition_date_utc
from ${env:TMP_db}.asp_m2dot0_apiagg_tmp${env:CLUSTER} x
  left join ${env:TMP_db}.asp_m2dot0_apiagg_tmp${env:CLUSTER} week1 on x.visit__application_details__application_name = week1.visit__application_details__application_name
    and x.AGG_visit__account__configuration_factors = week1.AGG_visit__account__configuration_factors
    and x.application__api__gql__operation_name = week1.application__api__gql__operation_name
    and cast(x.partition_date_utc as date) = week1.partition_date_utc
  left join ${env:TMP_db}.asp_m2dot0_apiagg_tmp${env:CLUSTER} week4 on x.visit__application_details__application_name = week4.visit__application_details__application_name
    and x.AGG_visit__account__configuration_factors = week4.AGG_visit__account__configuration_factors
    and x.application__api__gql__operation_name = week4.application__api__gql__operation_name
    and cast(x.partition_date_utc as date) = week4.partition_date_utc
WHERE (x.partition_date_utc >= '${env:STARTDATE_5day}' AND x.partition_date_utc < '${env:END_DATE}')
;

--------------------------------------------------------------------------------
---------------------- ***** DROP TEMPORARY TABLES ***** -----------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.visits_ranked_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.asp_m2dot0_apiagg_tmp${env:CLUSTER} PURGE;

--------------------------------------------------------------------------------
------------------------------- ***** END ***** --------------------------------
--------------------------------------------------------------------------------
