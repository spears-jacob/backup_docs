USE ${env:DASP_db};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join = true;

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


INSERT OVERWRITE TABLE si_agg_activation_channel PARTITION (denver_date)
select  accountnumber,date_level,user_package,application_group,
max(identity_created) as identity_created ,
max(login) as login,
max(setup_equipment) as setup_equipment,
max(get_started) as get_started,
max(accept_terms_conditions) as accept_terms_conditions, 
max(initiated_self_install) as initiated_self_install,
max(activation_failure) as activation_failure,
max(activation_success) as activation_success,
max(msa_collateral) as msa_collateral,
max(msa_elig_sawhomecard) as msa_elig_sawhomecard,
max(full_screen_takeover_page_view) as full_screen_takeover_page_view,
max(activation_complete) as activation_complete,
max(msa_activation_errors) as msa_activation_errors,
max(web_activation_errors) as web_activation_errors,
mso ,
application_type,
max(activation_success_partial)  as activation_success_partial,
denver_date
 from (select distinct a.acct_id as accountnumber, a.denver_date,a.date_level,a.user_package,a.application_group,a.mso,a.application_type, 
case when b.account_number is null then 'False' else 'True' end as identity_created, 
case when  c.login_eligible='1' then 'True' else 'False' end as login,
cast(case when cqe_ste.acct_id_ste is not null then 'True' else 'False' end  as varchar(50)) as setup_equipment,
cast(case when cqe_gts.acct_id_gts is not null then 'True' else 'False' end  as varchar(50)) as get_started, 
case when terms_conditions_accept>0 then 'True' else 'False' end as accept_terms_conditions, 
case when initiated_self_installs>0 then 'True' else 'False' end as initiated_self_install ,
case when activation_success_full>0 then 'True' else 'False' end as activation_success, 
case when activation_failure>0 then 'True' else 'False' end as activation_failure,
case when ce.encrypted_billing_account_number_256 is not null  then 'True' else 'False' end as msa_collateral,
case when home_page_view>0 then 'True' else 'False' end as msa_elig_sawhomecard,
case when full_screen_takeover_page_view>0 then 'True' else 'False' end as full_screen_takeover_page_view,
case when activation_complete>0 then 'True' else 'False' end as activation_complete,
case when msa_activation_errors='1' then 'True' else 'False' end as msa_activation_errors,
case when a.application_group = 'SelfInstall' and web.web_activation_errors='1'  then 'True' else 'False' end as web_activation_errors,
case when a.activation_success_partial>0 then 'True' else 'False' end as activation_success_partial
from 
(select * from ${env:DASP_db}.si_summary_agg_master
where  denver_date='${hiveconf:START_DATE}' and metric_type = 'Activation'
and application_group in ('MySpectrum', 'SelfInstall')
and date_level='daily'
) a 
left join (select distinct coalesced_account_number_256 as account_number from ${env:IDEN_db}.identity_ping_daily_snapshot 
where partition_date>='${hiveconf:PRIOR_8DAYS_START_DATE}' ) b on a.acct_id=b.account_number 
left join (select distinct denver_date,acct_id,'1' as login_eligible,application_group from ${env:DASP_db}.si_summary_page_base_master where denver_date='${hiveconf:START_DATE}' 
and event_case_id in ('mySpectrum_Manual_Login_Success',
'mySpectrum_Resume_Login_Success',
'selfInstall_login_manualSuccess',
'selfInstall_login_autoSuccess')) c on a.acct_id=c.acct_id and a.denver_date=c.denver_date and a.application_group=c.application_group
left join (select distinct visit__account__enc_account_number as acct_id_ste,partition_date_utc,visit__application_details__application_name from ${env:GLOBAL_db}.core_quantum_events_sspp
where partition_date_utc='${hiveconf:START_DATE}'
    and visit__application_details__application_name in ('MySpectrum', 'SelfInstall')
    and visit__account__enc_account_number is not null
    and message__event_case_id='mySpectrum_selectAction_homeCard_selfInstall_setupEquipment') cqe_ste  on cqe_ste.acct_id_ste=a.acct_id and a.denver_date=cqe_ste.partition_date_utc
	and cqe_ste.visit__application_details__application_name=a.application_group
left join (select distinct visit__account__enc_account_number as acct_id_gts,partition_date_utc,visit__application_details__application_name from ${env:GLOBAL_db}.core_quantum_events_sspp
where partition_date_utc='${hiveconf:START_DATE}'
    and visit__application_details__application_name in ('MySpectrum', 'SelfInstall')
    and visit__account__enc_account_number is not null
    and message__event_case_id='mySpectrum_selectAction_equipment_selfInstall_getStarted') cqe_gts  on cqe_gts.acct_id_gts=a.acct_id  and a.denver_date=cqe_gts.partition_date_utc
	and cqe_gts.visit__application_details__application_name=a.application_group
left join ${env:DASP_db}.si_etos_collateral ce on ce.encrypted_billing_account_number_256=a.acct_id and ce.partition_date_denver >= '${hiveconf:PRIOR_60DAYS_START_DATE}' and ce.partition_date_denver != '__HIVE_DEFAULT_PARTITION__'
left join (select distinct acct_id,'1' as msa_activation_errors,denver_date,application_group
from ${env:DASP_db}.si_summary_page_base_master where  denver_date='${hiveconf:START_DATE}'
and error_code in ('MSASI-0004','MSASI-0005','MSASI-1003','MSASI-1004','MSASI-1001') 
 ) mae on mae.acct_id=a.acct_id  and a.denver_date=mae.denver_date and a.application_group=mae.application_group
 left join (select distinct acct_id,'1' as web_activation_errors,denver_date,application_group
from ${env:DASP_db}.si_summary_page_base_master
where  denver_date='${hiveconf:START_DATE}' 
and event_case_id in ('selfInstall_error_verifyEquipmentSetup','selfInstall_error_deviceQualification_nonCOAM','selfInstall_error_deviceQualification_blacklistedCOAM','selfInstall_error_deviceQualification_minimallyQualifiedCOAM','selfInstall_error_missingEquipment','selfInstall_error_verifyDeviceOnline','selfInstall_error_verifyDeviceSignal','selfInstall_error_LOBOutage','selfInstall_error_acceptTermsConditions','selfInstall_error_portPhoneTransfer','selfInstall_error_callUs','selfInstall_error_tryAgain')) web  on web.acct_id=a.acct_id and 
a.denver_date=web.denver_date and a.application_group=web.application_group
)  fn
group by  accountnumber,denver_date,date_level,user_package,application_group,mso,application_type;