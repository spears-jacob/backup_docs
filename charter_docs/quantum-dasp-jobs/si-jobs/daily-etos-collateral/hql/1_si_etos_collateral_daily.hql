USE ${env:DASP_db};


set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join = false;
set hive.mapred.mode='strict';
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.cbo.enable=true;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.vectorized.execution.reduce.groupby.enabled = true;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=16;
set hive.tez.auto.reducer.parallelism=true;
set tez.grouping.max-size=78643200;
set tez.grouping.min-size=52428800;
set hive.exec.reducers.bytes.per.reducer=26214400;
set orc.force.positional.evolution=true;

SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;

ADD JAR ${env:JARS_S3_LOCATION}/epoch-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_converter AS 'Epoch_To_Denver';

ADD JAR ${env:JARS_S3_LOCATION}/jdatehour-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION epoch_datehour AS 'Epoch_Datehour';

CREATE TEMPORARY FUNCTION aes_encrypt256 AS 'com.spectrum.v1_0_10.hive.udfs.AesEncrypt256' USING JAR 's3://pi-global-${env:ENVIRONMENT}-udf-jars/hadoop-libs-hive-1.0.10.jar';



--DROP TABLE IF EXISTS ${env:TMP_db}.si_etos_collateral_incoming;
--CREATE  EXTERNAL TABLE IF NOT EXISTS ${env:TMP_db}.si_etos_collateral_incoming
--(
--  jde_item string, 
--  manufacturer_name string, 
--  head_end_component string, 
--  platform string, 
--  unit_type string, 
--  blling_model string, 
--  edw_standard string, 
--  serial_nbr string, 
--  pallet_nbr string, 
--  component_nbr string, 
--  picked_user string, 
--  shipped_user string, 
--  order_picked_date string, 
--  ship_stage_date string, 
--  system_ship_date string, 
--  carrier_ship_date string, 
--  carrier_delivery_date string, 
--  code_revision string, 
--  ship_to_loc string, 
--  street_addr string, 
--  city string, 
--  state string, 
--  location string, 
--  ezc_bau_facility_id string, 
--  ship_from_city string, 
--  ship_from_tier1 string, 
--  ship_form_tier1_city string, 
--  order_nbr string, 
--  mac_ua string, 
--  region string, 
--  bol string, 
--  dock_received_date string, 
--  created_date string, 
--  order_date string, 
--  unitqty string, 
--  billing_account_number string, 
--  billing_order_number string, 
--  cosys string, 
--  divprin string, 
--  headend string, 
--  site string, 
--  carrier string, 
--  tracking string, 
--  order_type string, 
--  customer_segment string, 
--  order_priority string, 
--  ship_to string, 
--  controller_name string, 
--  shipto_zip string, 
--  tpsi_ord_type string)
--ROW FORMAT DELIMITED 
--  FIELDS TERMINATED BY '|' 
--STORED AS INPUTFORMAT 
--  'org.apache.hadoop.mapred.TextInputFormat' 
--OUTPUTFORMAT 
--  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
--LOCATION
--  's3://pi-qtm-si-prod-aggregates-pii/data/prod_dasp/si_etos_collateral_incoming/'
--TBLPROPERTIES (
--  'has_encrypted_data'='false', 
--  'skip.header.line.count'='1');
--
--  
--  INSERT INTO ${env:DASP_db}.si_etos_collateral
--  select 
--   aes_encrypt256(a.billing_account_number, 'aes256')  as billing_account_number , 
--  a.billing_order_number , 
--  a.jde_item , 
--  a.manufacturer_name , 
--  a.head_end_component , 
--  a.platform , 
--  a.unit_type , 
--  a.blling_model , 
--  a.edw_standard , 
--  a.serial_nbr , 
--  a.pallet_nbr , 
--  a.component_nbr , 
--  a.order_picked_date , 
--  a.ship_stage_date , 
--  a.system_ship_date , 
--  a.carrier_ship_date , 
--  a.carrier_delivery_date , 
--  a.code_revision , 
--  a.ship_to_loc , 
--  a.ezc_bau_facility_id , 
--  a.ship_from_city , 
--  a.ship_from_tier1 , 
--  a.ship_form_tier1_city , 
--  a.order_nbr , 
--  a.region , 
--  a.bol , 
--  a.dock_received_date , 
--  a.created_date , 
--  a.order_date , 
--  null as short_order_date , 
--  a.unitqty , 
--  a.cosys , 
--  a.divprin , 
--  a.headend , 
--  a.site , 
--  a.carrier , 
--  a.order_type , 
--  a.customer_segment , 
--  a.order_priority , 
--  a.ship_to , 
--  a.controller_name , 
--  a.tpsi_ord_type,
--  '${hiveconf:START_DATE}' as last_updated_date from ${env:TMP_db}.si_etos_collateral_incoming a
--where  a.order_date not in (select distinct order_date from ${env:DASP_db}.si_etos_collateral )  ;
--  
--  DROP TABLE IF EXISTS ${env:TMP_db}.si_etos_collateral_incoming;
--
  
 INSERT OVERWRITE TABLE ${env:DASP_db}.si_summary_collateral_agg_nf PARTITION (order_date)
 select
		a.billing_account_number  , 
		a.created_date  , 
		a.short_order_date  ,
		a.region ,
		a.tpsi_ord_type ,
		b.mso ,  
		b.work_order_date  , 
		b.application_group  , 
		b.user_package  , 
		max(case when b.initiated_self_installs > 0 then 'True' else 'False' end) as si_flow_entry , 
		max(case when b.activation_success_full > 0 then 'True' else 'False' end) as order_full_success , 
		max(case when b.activation_success_partial > 0 then 'True' else 'False' end) as order_partial_success , 
		max(case when b.activation_success_full = 0 AND b.activation_success_partial = 0 then 'True' else 'False' end) as no_activation_success , 
		max(case when b.activation_complete > 0 then 'True' else 'False' end) as activation_complete,
		case when b.application_group='MySpectrum' then max( c.connectivity_check_msa) when b.application_group='SelfInstall' then max(tpsi.connectivity_check_tpsi) else 0 end as connectivity_check, 
		case when b.application_group='MySpectrum' then max(fnmod.message__feature__feature_name_cnt) when b.application_group='SelfInstall' then max(tpsi.activate_call_tpsi) else 0 end as activate_call , 
		case when b.application_group='MySpectrum' then max(c.activation_status_msa)  else 0 end as activation_status , 
		case when b.application_group='MySpectrum' then max(c.router_status_msa)  else 0 end as router_status ,
		case when b.application_group='MySpectrum' then max(fnmod.modem_message__feature__feature_name_cnt)
		when b.application_group='SelfInstall' then max(tpsi.connectivity_check_modem_tpsi)	
else 0 end  as connectivity_check_modem,
		case when b.application_group='MySpectrum'  then max(fnmod.router_message__feature__feature_name_cnt) 
		when b.application_group='SelfInstall' then max(tpsi.connectivity_check_router_tpsi)	
	else 0 end  as connectivity_check_router,
		case when b.application_group='MySpectrum'  then max(fnmod.receiver_message__feature__feature_name_cnt)
		when b.application_group='SelfInstall' then  max(tpsi.connectivity_check_receiver_tpsi)	
		else 0 end as connectivity_check_receiver,
		case when b.application_group='MySpectrum' then max(fnmod.modem_success_msa) when b.application_group='SelfInstall' then max(b.modem_success) else 0 end as modem_success,
		case when b.application_group='MySpectrum' then max(fnmod.router_success_msa) when b.application_group='SelfInstall' then max(b.router_success) else 0 end as router_success,
		case when b.application_group='MySpectrum' then max(fnmod.receiver_success_msa) when b.application_group='SelfInstall' then max(b.receiver_success) else 0 end as receiver_success,
		max(b.voice_setup_success) as voice_setup_success,		
		case when b.application_group='MySpectrum'  then max(fnmod.activate_call_modem_msa)
		when b.application_group='SelfInstall' then max(tpsi.activate_call_modem_tpsi)
		else 0 end as activate_call_modem,
		case when b.application_group='MySpectrum'  then max(fnmod.activate_call_router_msa) 
		when b.application_group='SelfInstall' then max(tpsi.activate_call_router_tpsi)
		else 0 end as activate_call_router,
		case when b.application_group='MySpectrum'  then max(fnmod.activate_call_receiver_msa) 
		when b.application_group='SelfInstall' then max(tpsi.activate_call_receiver_tpsi) else 0 end as activate_call_receiver,
		max(case when b.home_page_view>0 then 'True' else 'False' end) as digital_interaction,
		'True' as collateral,  
		b.denver_date,
		max(c.modem_attempt_msa) as modem_attempt,
		max(c.router_attempt_msa) as router_attempt,
		max(c.reciever_attempt_msa) as reciever_attempt,
		a.order_date as order_date
  from (select distinct encrypted_billing_account_number_256 as billing_account_number,
  created_date_utc as created_date,
  substr(order_date_utc,1,10) as order_date,
  null as short_order_date,
  region,
  tpsi_ord_type from  ${env:DASP_db}.si_etos_collateral
 where substr(order_date_utc,1,10) >='${hiveconf:PRIOR_15DAYS_START_DATE}' and partition_date_denver >= '${hiveconf:PRIOR_15DAYS_START_DATE}' and partition_date_denver != '__HIVE_DEFAULT_PARTITION__') a 
  left join (select acct_id, application_group  ,mso,work_order_date,denver_date, 
		user_package ,max(initiated_self_installs) as initiated_self_installs ,max(activation_success_partial) as activation_success_partial,
		max(activation_success_full) as activation_success_full ,max(activation_complete) as activation_complete,
		sum(if(modem_setup_success>0 and application_group='SelfInstall',1,0)) as modem_success,
		sum(if(router_setup_success>0 and application_group='SelfInstall',1,0)) as router_success,
		sum(if(tv_setup_success>0 and application_group='SelfInstall',1,0)) as receiver_success,
		sum(if(voice_setup_success>0,1,0)) as voice_setup_success,
		max(home_page_view) as home_page_view
		from ${env:DASP_db}.si_summary_agg_master
		where denver_date>='${hiveconf:PRIOR_15DAYS_START_DATE}'
		and date_level = 'daily' and metric_type = 'Activation'
         and acct_id in (select distinct encrypted_billing_account_number_256 as billing_account_number from  ${env:DASP_db}.si_etos_collateral
 where  substr(order_date_utc,1,10) >='${hiveconf:PRIOR_15DAYS_START_DATE}' and partition_date_denver >= '${hiveconf:PRIOR_15DAYS_START_DATE}' and partition_date_denver != '__HIVE_DEFAULT_PARTITION__')
		group by   acct_id,application_group  , user_package,mso,work_order_date,denver_date ) b on a.billing_account_number=b.acct_id  
  left join 
	(select 
	visit__account__enc_account_number,
	visit__application_details__application_name, 
	cast(cast(from_utc_timestamp(received__timestamp,'MST') as date) as string) as received_dt,
	sum(if((application__api__api_name = 'equipmentActivationV1ConnectivityCheck' OR application__api__path like '%/api/pub/equipment-activation/v1/connectivity/%'),1,0))  as connectivity_check_msa,
	sum(if(application__api__api_name = 'equipmentActivationV1Status',1,0)) as activation_status_msa,
	sum(if(application__api__api_name = 'equipmentV3LobInternet' and state__view__current_page__page_name = 'selfInstallRouterSetup',1,0))  as router_status_msa,
sum(if(message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_modemSetup',1,0)) as modem_attempt_msa,
		sum(if(message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_routerSetup',1,0)) as router_attempt_msa,
		sum(if(message__event_case_id = 'mySpectrum_pageView_equipment_selfInstall_receiverSetup',1,0)) as reciever_attempt_msa	
	   from ${env:GLOBAL_db}.core_quantum_events_sspp
	   where  partition_date_utc >='${hiveconf:PRIOR_15DAYS_START_DATE}'
	   and visit__account__enc_account_number in (select distinct encrypted_billing_account_number_256 as billing_account_number from  ${env:DASP_db}.si_etos_collateral
 where  substr(order_date_utc,1,10)>='${hiveconf:PRIOR_15DAYS_START_DATE}' and partition_date_denver >= '${hiveconf:PRIOR_15DAYS_START_DATE}' and partition_date_denver != '__HIVE_DEFAULT_PARTITION__')
	   and visit__application_details__application_name  in ('MySpectrum','SelfInstall') 
	   group by visit__account__enc_account_number,	visit__application_details__application_name, 
	cast(cast(from_utc_timestamp(received__timestamp,'MST') as date) as string)
	  	  ) c on a.billing_account_number=c.visit__account__enc_account_number 
			and c.received_dt = b.denver_date 
			and c.visit__application_details__application_name = b.application_group
 --adding the following  join  as part of XGANALYTIC-29410 
 left join 
	(select  visit__account__enc_account_number,visit__application_details__application_name,
cast(cast(from_utc_timestamp(received__timestamp,'MST') as date) as string) as received_dt 
	,sum(if(message__event_case_id ='mySpectrum_applicationActivity_equipment_selfInstall_status' and message__feature__feature_name in ('modem','router','receiver') and state__view__current_page__elements__element_string_value = 'connecting' ,1,0))  as message__feature__feature_name_cnt
	,sum(if(message__event_case_id='mySpectrum_applicationActivity_equipment_selfInstall_status'
and state__view__current_page__elements__element_string_value='connecting'
and next_page='selfInstallModemSetup',1,0))  as modem_message__feature__feature_name_cnt
    ,sum(if(message__event_case_id='mySpectrum_applicationActivity_equipment_selfInstall_status'
and state__view__current_page__elements__element_string_value='connecting'
and next_page='selfInstallRouterSetup'
	and visit__account__details__mso not IN('CHARTER','CHTR','CHTR','CHR'),1,0)) as router_message__feature__feature_name_cnt
,sum(if(message__event_case_id ='mySpectrum_applicationActivity_equipment_selfInstall_status' and (message__feature__feature_name = 'receiver' or  state__view__current_page__page_name in ('selfInstallReceiverSetup','Insert Batteries Into Remote')) and state__view__current_page__elements__element_string_value = 'connecting' ,1,0))  as receiver_message__feature__feature_name_cnt
	,sum(if(message__event_case_id ='mySpectrum_applicationActivity_equipment_selfInstall_status' and (message__feature__feature_name in ('modem','router','receiver')   or  state__view__current_page__page_name in ('selfInstallRouterSetup','selfInstallModemSetup','selfInstallReceiverSetup','Insert Batteries Into Remote'))and state__view__current_page__elements__element_string_value = 'activating' ,1,0)) as activate_call_msa
    ,sum(if(message__event_case_id ='mySpectrum_applicationActivity_equipment_selfInstall_status' and (message__feature__feature_name = 'modem'  or  state__view__current_page__page_name in ('selfInstallModemSetup','selfInstallVerifyYourModem','Connect Power Cable')) and state__view__current_page__elements__element_string_value = 'activating' ,1,0)) as activate_call_modem_msa
    ,sum(if(message__event_case_id ='mySpectrum_applicationActivity_equipment_selfInstall_status' and (message__feature__feature_name = 'router'  or state__view__current_page__page_name in ('selfInstallRouterSetup')) and state__view__current_page__elements__element_string_value = 'activating' and visit__account__details__mso not IN('CHARTER','CHTR','CHTR','CHR'),1,0)) as activate_call_router_msa
    ,sum(if(message__event_case_id ='mySpectrum_applicationActivity_equipment_selfInstall_status' and (message__feature__feature_name = 'receiver' or  state__view__current_page__page_name in ('selfInstallReceiverSetup','Insert Batteries Into Remote')) and state__view__current_page__elements__element_string_value = 'activating' ,1,0)) as activate_call_receiver_msa
    ,sum(if(message__event_case_id ='mySpectrum_applicationActivity_equipment_selfInstall_status' and (message__feature__feature_name = 'modem'  or  state__view__current_page__page_name in ('selfInstallModemSetup','selfInstallVerifyYourModem','Connect Power Cable')) and state__view__current_page__elements__element_string_value = 'finished',1,0)) as modem_success_msa
    ,sum(if(message__event_case_id ='mySpectrum_applicationActivity_equipment_selfInstall_status' and (message__feature__feature_name = 'router'  or state__view__current_page__page_name in ('selfInstallRouterSetup')) and state__view__current_page__elements__element_string_value = 'finished' and visit__account__details__mso not IN('CHARTER','CHTR','CHTR','CHR'),1,0)) as router_success_msa
    ,sum(if(message__event_case_id ='mySpectrum_applicationActivity_equipment_selfInstall_status' and (message__feature__feature_name = 'receiver' or  state__view__current_page__page_name in ('selfInstallReceiverSetup','Insert Batteries Into Remote')) and state__view__current_page__elements__element_string_value = 'finished',1,0)) as receiver_success_msa 

from
	(select visit__account__enc_account_number,visit__application_details__application_name,
	received__timestamp,message__event_case_id,message__feature__feature_name,
	state__view__current_page__elements__element_string_value,
	visit__account__details__mso,state__view__current_page__page_name
	,LEAD(state__view__current_page__page_name,1, '')
 OVER (PARTITION BY visit__application_details__application_name, visit__account__enc_account_number, visit__visit_id ORDER BY message__sequence_number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as next_page
from  ${env:GLOBAL_db}.core_quantum_events_sspp
 where partition_date_utc>='${hiveconf:PRIOR_15DAYS_START_DATE}'
and visit__application_details__application_name ='MySpectrum'
) a
 group by visit__account__enc_account_number,visit__application_details__application_name,
cast(cast(from_utc_timestamp(received__timestamp,'MST') as date) as string)
  ) fnmod on a.billing_account_number=fnmod.visit__account__enc_account_number 
			and fnmod.received_dt = b.denver_date 
			and fnmod.visit__application_details__application_name = b.application_group

left join 			
(select visit__account__enc_account_number,visit__application_details__application_name,
cast(cast(from_utc_timestamp(received__timestamp,'MST') as date) as string) as received__dt,
sum(if((application__api__path='/auto-activation-device-online/device-online-statuses'  AND si_data LIKE '%"pendingServices":["%"]%'),1,0)) as connectivity_check_tpsi,
sum(if(application__api__path = '/auto-activation/activate-service-requests' AND si_data LIKE '%"pendingServices":["%"]%',1,0))  as activate_call_tpsi,
sum(if((application__api__path='/auto-activation-device-online/device-online-statuses'  AND si_data LIKE '%"pendingServices":["%"]%' AND (si_data LIKE '%EMTA%' OR LOWER(si_data) LIKE '%modem%')),1,0)) as connectivity_check_modem_tpsi,
sum(if((application__api__path='/auto-activation-device-online/device-online-statuses'  AND si_data LIKE '%"pendingServices":["%"]%' AND (si_data LIKE '%AWG%' OR LOWER(si_data) LIKE '%router%')),1,0))  as connectivity_check_router_tpsi,
sum(if((application__api__path='/auto-activation-device-online/device-online-statuses'  AND si_data LIKE '%"pendingServices":["%"]%' AND (si_data LIKE '%STB%' OR LOWER(si_data) LIKE '%receiver%')),1,0)) as connectivity_check_receiver_tpsi,
sum(if((application__api__path='/auto-activation/activate-service-requests'  AND si_data LIKE '%"pendingServices":["%"]%' AND (si_data LIKE '%EMTA%' OR LOWER(si_data) LIKE '%modem%')),1,0)) as activate_call_modem_tpsi,
sum(if((application__api__path='/auto-activation/activate-service-requests'  AND si_data LIKE '%"pendingServices":["%"]%' AND (si_data LIKE '%AWG%' OR LOWER(si_data) LIKE '%router%')),1,0)) as activate_call_router_tpsi,
sum(if((application__api__path='/auto-activation/activate-service-requests'  AND si_data LIKE '%"pendingServices":["%"]%' AND (si_data LIKE '%STB%' OR LOWER(si_data) LIKE '%receiver%')),1,0)) as activate_call_receiver_tpsi
from 
(select distinct visit__account__enc_account_number,visit__application_details__application_name,received__timestamp,application__api__path,si_data
from ${env:GLOBAL_db}.core_quantum_events_sspp base  LATERAL VIEW explode(split(state__view__current_page__additional_information,'},')) explodeVal AS si_data
where  base.partition_date_utc>='${hiveconf:PRIOR_15DAYS_START_DATE}'
and base.visit__account__enc_account_number in (select distinct encrypted_billing_account_number_256 as billing_account_number from  ${env:DASP_db}.si_etos_collateral
 where  substr(order_date_utc,1,10)>='${hiveconf:PRIOR_15DAYS_START_DATE}' and partition_date_denver >= '${hiveconf:PRIOR_15DAYS_START_DATE}' and partition_date_denver != '__HIVE_DEFAULT_PARTITION__')
AND base.visit__application_details__application_type='Web'
AND base.visit__application_details__application_name='SelfInstall'
AND base.application__api__path IN ('/auto-activation/activate-service-requests', '/auto-activation-device-online/device-online-statuses')
) a group by visit__account__enc_account_number,visit__application_details__application_name,
cast(cast(from_utc_timestamp(received__timestamp,'MST') as date) as string)) tpsi
 on a.billing_account_number=tpsi.visit__account__enc_account_number 
			and tpsi.received__dt = b.denver_date 
			and tpsi.visit__application_details__application_name = b.application_group
group by  a.billing_account_number  , 
a.created_date  , 
a.order_date , 
a.short_order_date  ,
a.region ,
a.tpsi_ord_type ,
b.mso ,  
b.work_order_date  , 
b.application_group  , 
b.user_package, 
b.denver_date;