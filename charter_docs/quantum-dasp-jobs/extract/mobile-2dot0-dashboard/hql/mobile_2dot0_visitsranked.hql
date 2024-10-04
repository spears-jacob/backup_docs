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
-------------------------- ***** Visits Ranked ***** ---------------------------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_m2dot0_visitsRanked
SELECT DISTINCT
portals_unique_acct_key as visit__account__enc_account_number
,AGG_visit__account__details__mso
,visit__account__enc_mobile_account_number
,visit__application_details__application_name
,visit__application_details__application_type
,visit__application_details__app_version
,AGG_visit__account__configuration_factors
,AGG_CUSTOM_visit__account__details__service_subscriptions
,AGG_CUSTOM_customer_group
,visit_id as visit__visit_id
,visit__visit_start_timestamp
,ROW_NUMBER()
  OVER(PARTITION BY portals_unique_acct_key, visit__account__enc_mobile_account_number, AGG_visit__account__details__mso, visit__application_details__application_name, AGG_visit__account__configuration_factors
        ORDER BY visit__visit_start_timestamp) as CUSTOM_visit_num_acctMSOappMS
,partition_date_utc
from `${env:M2MA}`
where (partition_date_utc >= '2021-09-01')
  AND visit_id is not null
  and portals_unique_acct_key is not null
  and visit__account__enc_mobile_account_number is not null
  and AGG_visit__account__details__mso is not null
  and AGG_CUSTOM_customer_group = 'Mobile 2.0 Customer'
;

--------------------------------------------------------------------------------
------------------------------- ***** END ***** --------------------------------
--------------------------------------------------------------------------------
