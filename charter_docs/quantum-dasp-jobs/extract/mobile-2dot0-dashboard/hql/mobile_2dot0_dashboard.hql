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

-- test comment gamma

--------------------------------------------------------------------------------
------------------------- ***** Load Partitions ***** --------------------------
--------------------------------------------------------------------------------

MSCK REPAIR TABLE `${env:CPVC}`;

--------------------------------------------------------------------------------
------------ ***** Create temporary tables for job data sets ***** -------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.calls_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.calls_${env:CLUSTER} AS
SELECT DISTINCT
visit_type,
visit_id,
account_number,
account_agent_mso,
call_inbound_key,
minutes_to_call
from `${env:CPVC}` c
where (call_date >= '${env:STARTDATE_2week}' AND call_date < '${env:END_DATE}')
;

DROP TABLE IF EXISTS ${env:TMP_db}.calls2_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.calls2_${env:CLUSTER} AS
select
visit_id,
call_inbound_key,
account_number,
account_agent_mso,
minutes_to_call,
ROW_NUMBER() OVER(PARTITION BY visit_type, call_inbound_key ORDER BY minutes_to_call) as visit_call_rank
from ${env:TMP_db}.calls_${env:CLUSTER}
;

DROP TABLE IF EXISTS ${env:TMP_db}.v_c_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.v_c_${env:CLUSTER} AS
select distinct
ma.portals_unique_acct_key,
ma.Agg_visit__account__details__mso,
ma.visit__account__enc_mobile_account_number,
ma.visit__application_details__application_name,
ma.visit__application_details__app_version,
ma.AGG_visit__account__configuration_factors,
ma.AGG_CUSTOM_customer_group,
ma.visit_id,
ma.visit__visit_start_timestamp,
vr.CUSTOM_visit_num_acctMSOappMS,
case when c.visit_id is not null and c.visit_call_rank = 1 then 1 else 0 end as CUSTOM_visit_had_call,
case when c.visit_id is not null then 1 else 0 end as CUSTOM_visit_had_call_inclusive,
ma.partition_date_utc
from `${env:M2MA}` ma
  left join ${env:DASP_db}.asp_m2dot0_visitsRanked vr
--  LEFT JOIN ${env:TMP_db}.visits_ranked_${env:CLUSTER} vr
    on ma.portals_unique_acct_key = visit__account__enc_account_number
    and ma.AGG_visit__account__details__mso = vr.AGG_visit__account__details__mso
    and ma.AGG_CUSTOM_customer_group = vr.agg_custom_customer_group
    and ma.visit_id = vr.visit__visit_id
    and ma.partition_date_utc = vr.partition_date_utc
  left join ${env:TMP_db}.calls2_${env:CLUSTER} c
    on ma.visit_id = c.visit_id
    and ma.portals_unique_acct_key = c.account_number
    and case
          when ma.AGG_visit__account__details__mso = 'CHTR' then 'CHR'
          when ma.AGG_visit__account__details__mso = 'CHARTER' then 'CHR'
          when ma.AGG_visit__account__details__mso = 'BH' then 'BHN'
          when ma.AGG_visit__account__details__mso like '%TWC%' then 'TWC'
          else ma.AGG_visit__account__details__mso end
          = c.account_agent_mso
where (ma.partition_date_utc >= '${env:STARTDATE_2week}' AND ma.partition_date_utc < '${env:END_DATE}')
  AND ma.visit_id is not null
  and ma.portals_unique_acct_key is not null
  and ma.AGG_visit__account__details__mso is not null
  and ma.AGG_visit__account__details__mso <> 'unknown'
;

--------------------------------------------------------------------------------
----------------------------- ***** CIR Agg ***** ------------------------------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_m2dot0_CIRagg PARTITION (partition_date_utc)
select
AGG_visit__account__details__mso
,visit__application_details__application_name
,AGG_visit__account__configuration_factors
,AGG_CUSTOM_customer_group
,count(distinct visit_id) as visits
,sum(CUSTOM_visit_had_call) as visits_with_call
,sum(CUSTOM_visit_had_call_inclusive) as visits_with_call_inclusive
,sum(if(CUSTOM_visit_num_acctMSOappMS = 1,1,0)) as visits_first
,sum(if(CUSTOM_visit_num_acctMSOappMS = 1,CUSTOM_visit_had_call_inclusive,0)) as visits_first_with_call_inclusive
,partition_date_utc
from ${env:TMP_db}.v_c_${env:CLUSTER}
group by
AGG_visit__account__details__mso
,visit__application_details__application_name
,AGG_visit__account__configuration_factors
,AGG_CUSTOM_customer_group
,partition_date_utc
;

--------------------------------------------------------------------------------
------------------------ ***** Engaged Households ***** ------------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.mvno_accounts_exclude_old_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.mvno_accounts_exclude_old_${env:CLUSTER} AS
select
*
from `${env:MSAH}`
where partition_date_et = '2022-06-14'
;

DROP TABLE IF EXISTS ${env:TMP_db}.mvno_accounts_exclude_new_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.mvno_accounts_exclude_new_${env:CLUSTER} AS
select
*
from `${env:MSAH}`
where partition_date_et = '2022-06-15'
;

DROP TABLE IF EXISTS ${env:TMP_db}.mvno_accounts_exclude_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.mvno_accounts_exclude_${env:CLUSTER} AS
select
o.mobl_acct_key_aes256
from ${env:TMP_db}.mvno_accounts_exclude_new_${env:CLUSTER} n
left join ${env:TMP_db}.mvno_accounts_exclude_old_${env:CLUSTER} o on (n.mobl_acct_key_aes256 = o.mobl_acct_key_aes256)
where o.src_sys_id = '101'
and n.src_sys_id = '102'
and o.bi_account_status = 'Deactivated'
and n.bi_account_status = 'Open'
;

DROP TABLE IF EXISTS ${env:TMP_db}.mvno_accounts_raw_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.mvno_accounts_raw_${env:CLUSTER} AS
select
mobl_acct_key_aes256
,cast(src_sys_id as int) as src_sys_id
,partition_date_et
from `${env:MSAC}`
where biller_account_status in ('AC','TD')
and test_prod_ind = 'PROD'
;

DROP TABLE IF EXISTS ${env:TMP_db}.mvno_accounts_agg_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.mvno_accounts_agg_${env:CLUSTER} AS
select
mobl_acct_key_aes256
,SUM(src_sys_id) as SUM_src_sys_id
,partition_date_et
from ${env:TMP_db}.mvno_accounts_raw_${env:CLUSTER}
group by
mobl_acct_key_aes256
,partition_date_et
;

DROP TABLE IF EXISTS ${env:TMP_db}.mvno_counts_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.mvno_counts_${env:CLUSTER} AS
select
a.SUM_src_sys_id
,COUNT(1) as mvno_accounts
,a.partition_date_et
from ${env:TMP_db}.mvno_accounts_agg_${env:CLUSTER} a
left join ${env:TMP_db}.mvno_accounts_exclude_${env:CLUSTER} ex
  on (a.mobl_acct_key_aes256 = ex.mobl_acct_key_aes256 and
      a.partition_date_et > '2022-06-14')
where ex.mobl_acct_key_aes256 is null
group by
a.SUM_src_sys_id
,a.partition_date_et
;


DROP TABLE IF EXISTS ${env:TMP_db}.vr_joiner_${env:CLUSTER} PURGE;
CREATE TEMPORARY TABLE ${env:TMP_db}.vr_joiner_${env:CLUSTER} AS
select
partition_date_utc
,'Both Apps' as visit__application_details__application_name
--,AGG_visit__account__configuration_factors
,sum(both_first_visits) as first_visits
from
  (select
    visit__account__enc_account_number
    ,AGG_visit__account__details__mso
    ,visit__account__enc_mobile_account_number
    ,visit__application_details__application_name
    ,visit__application_details__application_type
    ,visit__application_details__app_version
    ,AGG_visit__account__configuration_factors
    ,AGG_CUSTOM_visit__account__details__service_subscriptions
    ,AGG_CUSTOM_customer_group
    ,visit__visit_id
    ,visit__visit_start_timestamp
    ,CUSTOM_visit_num_acctMSOappMS
    ,if(sum(custom_visit_num_acctmsoappms)
      over(partition by visit__account__enc_account_number, agg_visit__account__details__mso, visit__account__enc_mobile_account_number
          order by partition_date_utc
          rows between unbounded preceding and current row)
          = 2, 1, 0) as both_first_visits
    ,partition_date_utc
    from ${env:DASP_db}.asp_m2dot0_visitsRanked
--  FROM ${env:TMP_db}.visits_ranked_${env:CLUSTER}
    where custom_visit_num_acctmsoappms = 1
    and visit__application_details__application_name in ('SpecNet', 'MySpectrum')
  ) sub
group by
partition_date_utc
--,AGG_visit__account__configuration_factors
;

INSert INTO ${env:TMP_db}.vr_joiner_${env:CLUSTER}
select
partition_date_utc
,'Any Apps' as visit__application_details__application_name
--,AGG_visit__account__configuration_factors
,sum(1) as first_visits
from
  ( select
    visit__account__enc_account_number,
    agg_visit__account__details__mso,
    visit__account__enc_mobile_account_number,
    --AGG_visit__account__configuration_factors,
    min(partition_date_utc) as partition_date_utc
  from ${env:DASP_db}.asp_m2dot0_visitsRanked
--  FROM ${env:TMP_db}.visits_ranked_${env:CLUSTER}
    where custom_visit_num_acctmsoappms = 1
    group by
    visit__account__enc_account_number,
    agg_visit__account__details__mso,
    visit__account__enc_mobile_account_number--,
    --AGG_visit__account__configuration_factors
  ) sub
group by
partition_date_utc
--,AGG_visit__account__configuration_factors
;

INSert INTO ${env:TMP_db}.vr_joiner_${env:CLUSTER}
select
partition_date_utc,
visit__application_details__application_name,
--AGG_visit__account__configuration_factors,
sum(1) as first_visits
from ${env:DASP_db}.asp_m2dot0_visitsRanked
--FROM ${env:TMP_db}.visits_ranked_${env:CLUSTER}
where custom_visit_num_acctmsoappms = 1
group by
partition_date_utc,
visit__application_details__application_name--,
--AGG_visit__account__configuration_factors
;

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_m2dot0_engagedhouseholds
select
x.visit__application_details__application_name
--,x.AGG_visit__account__configuration_factors
,MAX(x.first_visits) as first_visits
,m.mvno_accounts
,m.SUM_src_sys_id
,x.partition_date_utc as date_date
from ${env:TMP_db}.vr_joiner_${env:CLUSTER} x
left join ${env:TMP_db}.mvno_counts_${env:CLUSTER} m on x.partition_date_utc = m.partition_date_et
group by
x.partition_date_utc
,x.visit__application_details__application_name
--,x.AGG_visit__account__configuration_factors
,m.mvno_accounts
,m.SUM_src_sys_id
;

--------------------------------------------------------------------------------
----------------------- ***** Metric Visit Lookup ***** ------------------------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_m2dot0_metric_visit_lookup
PARTITION (partition_date_utc)
SELECT
    visit__application_details__application_name,
    visit__application_details__application_type,
    visit__application_details__app_version,
    agg_custom_visit__account__details__service_subscriptions,
    agg_custom_customer_group,
    agg_visit__account__configuration_factors,
    visit_id,
    metric_name,
    metric_value,
    partition_date_utc
  FROM
    (
    SELECT
      partition_date_utc,
      visit__application_details__application_name,
      visit__application_details__application_type,
      visit__application_details__app_version,
      agg_custom_visit__account__details__service_subscriptions,
      agg_custom_customer_group,
      agg_visit__account__configuration_factors,
      visit_id,
        MAP(

--------------------------------------------------------------------------------
---------------------------- ***** The Rates ***** -----------------------------
--------------------------------------------------------------------------------
     'Activation Success Rate', sum(mobile_activation_activation_complete)/sum(mobile_activation_activation_in_progress + mobile_activation_transfer_in_progress),
     'AutoPay Enroll Flow Completion Rate (Core)', SUM(AP_enroll_success_core)/sum(AP_enroll_start_core),
     'AutoPay Enroll Attempt Success Rate (Core)', sum(ap_enroll_success_core)/SUM(ap_enroll_success_core + AP_enroll_failure_core),
     'Device Payment Flow Completion Rate', SUM(device_make_payment_success)/SUM(device_make_payment_start),
     'Device Payment Submit Success Rate', SUM(device_make_payment_success)/SUM(device_make_payment_submit),
     'Device Payment Attempt Success Rate', SUM(device_make_payment_success)/SUM(device_make_payment_success + device_make_payment_failure),
     'Device Upgrade Starts', SUM(mobile_device_upgrade),
     'Login Success Rate', sum(login_success)/sum(login_attempt),
     'OTP Flow Completion Rate (Core)', SUM(OTP_success_core + OTP_success_core_full + OTP_success_core_quickPay + OTP_success_legacy)/sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
     'OTP Flow Completion Rate (Mobile)', sum(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile)/sum(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
     'OTP Submit Success Rate (Core)', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy)/SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
     'OTP Submit Success Rate (Mobile)', sum(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile)/SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
     'OTP Attempt Success Rate (Core)', SUM(OTP_success_core + OTP_success_core_full + OTP_success_core_quickPay + OTP_success_legacy)/
                                       SUM(OTP_success_core + OTP_success_core_full + OTP_success_core_quickPay + OTP_success_legacy + OTP_failure_core + OTP_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
     'OTP Attempt Success Rate (Mobile)', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay)/
                                         SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),
     'Page Load Time (Home)', sum(page_load_home - page_load_home_hot_3000)/sum(page_load_home),
     'Page Load Time (Billing)', SUM(page_load_billing - page_load_billing_hot_3000)/sum(page_load_billing),
     'CPNI Flow Completion Rate', SUM(CPNI_success)/SUM(CPNI_start + CPNI_start_modalView + CPNI_start_pageView),
     'CPNI Submit Success Rate', sum(CPNI_success)/SUM(CPNI_submit),
     'CPNI Attempt Success Rate', SUM(CPNI_success)/sum(CPNI_success + CPNI_failure),
     'Mobile Change Plan Flow Completion Rate', sum(mobile_tier_upgrade_success + mobile_success_change_plan)/SUM(mobile_tier_upgrade_start + mobile_change_plan_start),
     'Mobile Change Plan Submit Success Rate', sum(mobile_tier_upgrade_success + mobile_success_change_plan)/SUM(mobile_confirm_change_plan + mobile_tier_upgrade_submit),
     -- 'CPNI Submit Success Rate (IMEI)', sum(CPNI_success_imei)/sum(CPNI_submit_imei),
     -- 'CPNI Attempt Success Rate (IMEI)', SUM(CPNI_success_imei)/sum(CPNI_success_imei + CPNI_failure_IMEI),
     -- 'CPNI Submit Success Rate (MAC Address)', sum(CPNI_success_mac_address)/sum(CPNI_submit_mac_address),
     -- 'CPNI Attempt Success Rate (Mac Address)', SUM(CPNI_success_mac_address)/SUM(CPNI_success_mac_address + CPNI_failure_mac_address),
     -- 'CPNI Submit Success Rate (Email)', sum(CPNI_success_email)/sum(CPNI_submit_email),
     -- 'CPNI Attempt Success Rate (Email)', SUM(CPNI_success_email)/sum(CPNI_success_email + CPNI_failure_email),
     -- 'CPNI Submit Success Rate (Security Code)', sum(CPNI_success_security_code)/sum(CPNI_submit_security_code),
     -- 'CPNI Attempt Success Rate (Security Code)', sum(CPNI_success_security_code)/sum(CPNI_success_security_code + CPNI_failure_security_code),

--------------------------------------------------------------------------------
--------------- ***** The Components of Reporting Metrics ***** ----------------
--------------------------------------------------------------------------------
     'CPNI Flow Completion - Starts', sum(cpni_start + CPNI_start_pageView + CPNI_start_modalView),
     'CPNI Flow Completion - Submits', sum(CPNI_submit),
     'CPNI Flow Completion - Successes', sum(CPNI_success),
     'CPNI Flow Completion - Failures', sum(CPNI_failure),
     'CPNI Flow Completion - Attempts', sum(CPNI_success + CPNI_failure),
     'CPNI Submit Success - Starts', sum(cpni_start + CPNI_start_pageView + CPNI_start_modalView),
     'CPNI Submit Success - Submits', sum(CPNI_submit),
     'CPNI Submit Success - Successes', sum(CPNI_success),
     'CPNI Submit Success - Failures', sum(CPNI_failure),
     'CPNI Submit Success - Attempts', sum(CPNI_success + CPNI_failure),
     'CPNI Attempt Success - Starts', sum(cpni_start + CPNI_start_pageView + CPNI_start_modalView),
     'CPNI Attempt Success - Submits', sum(CPNI_submit),
     'CPNI Attempt Success - Successes', sum(CPNI_success),
     'CPNI Attempt Success - Failures', sum(CPNI_failure),
     'CPNI Attempt Success - Attempts', sum(CPNI_success + CPNI_failure),
     'Activation Successes', sum(mobile_activation_activation_complete),
     'Activation Starts', SUm(mobile_activation_activation_in_progress + mobile_activation_transfer_in_progress),
     'Activation Failures', SUM(mobile_activation_unable_to_activate),
     'AutoPay Enroll Flow (Core) - Starts', SUM(ap_enroll_start_core),
     'AutoPay Enroll Flow (Core) - Successes', SUM(AP_enroll_success_core),
     'AutoPay Enroll Flow (Core) - Failures', SUM(ap_enroll_failure_core),
     'AutoPay Enroll Flow (Core) - Attempts', SUM(ap_enroll_success_core + ap_enroll_failure_core),
     --'AutoPay Enroll Flow (Core) - Abandons', sum(ap_enroll_abandon_core),
     'AutoPay Enroll Attempt (Core) - Starts', sum(ap_enroll_start_core),
     'AutoPay Enroll Attempt (Core) - Successes', sum(AP_enroll_success_core),
     'AutoPay Enroll Attempt (Core) - Failures', sum(ap_enroll_failure_core),
     'AutoPay Enroll Attempt (Core) - Attempts', SUM(ap_enroll_failure_core + AP_enroll_success_core),
     --'AutoPay Enroll Attempt (Core) - Abandons', sum(ap_enroll_abandon_core),
     'Device Payment Flow Completion - Successes', sum(device_make_payment_success),
     'Device Payment Flow Completion - Starts', sum(device_make_payment_start),
     'Device Payment Flow Completion - Failures', SUM(device_make_payment_failure),
     'Device Payment Flow Completion - Attempts', sum(device_make_payment_failure + device_make_payment_success),
     'Device Payment Flow Completion - Submit', sum(device_make_payment_submit),
     'Device Payment Submit Success - Successes', sum(device_make_payment_success),
     'Device Payment Submit Success - Starts', sum(device_make_payment_start),
     'Device Payment Submit Success - Failures', SUM(device_make_payment_failure),
     'Device Payment Submit Success - Attempts', sum(device_make_payment_failure + device_make_payment_success),
     'Device Payment Submit Success - Submits', sum(device_make_payment_submit),
     'Device Payment Attempt Success - Successes', sum(device_make_payment_success),
     'Device Payment Attempt Success - Starts', sum(device_make_payment_start),
     'Device Payment Attempt Success - Failures', SUM(device_make_payment_failure),
     'Device Payment Attempt Success - Attempts', sum(device_make_payment_failure + device_make_payment_success),
     'Device Payment Attempt Success - Submit', sum(device_make_payment_submit),
     'Login Successes', sum(login_success),
     'Login Failures', sum(login_failure),
     'Login Attempts', sum(login_success + login_failure),
     'Mobile Change Plan Flow Completion - Successes', sum(mobile_tier_upgrade_success + mobile_success_change_plan),
     'Mobile Change Plan Flow Completion - Submits', SUM(mobile_confirm_change_plan + mobile_tier_upgrade_submit),
     'Mobile Change Plan Flow Completion - Starts', sum(mobile_tier_upgrade_start + mobile_change_plan_start),
     'Mobile Change Plan Submit Success Rate - Successes', sum(mobile_tier_upgrade_success + mobile_success_change_plan),
     'Mobile Change Plan Submit Success Rate - Submits', SUM(mobile_confirm_change_plan + mobile_tier_upgrade_submit),
     'Mobile Change Plan Submit Success Rate - Starts', sum(mobile_tier_upgrade_start + mobile_change_plan_start),

     'OTP Flow Completion (Core) - Successes', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy),
     'OTP Flow Completion (Core) - Starts', sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
     'OTP Flow Completion (Core) - Failures', SUM(otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
     'OTP Flow Completion (Core) - Submits', SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
     'OTP Flow Completion (Core) - Attempts', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy + otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),

     'OTP Attempt Success Rate (Core) - Successes',  sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy),
     'OTP Attempt Success Rate (Core) - Starts', sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
     'OTP Attempt Success Rate (Core) - Failures', SUM(otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
     'OTP Attempt Success Rate (Core) - Submits', SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
     'OTP Attempt Success Rate (Core) - Attempts', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy + otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),

     'OTP Submit Success Rate (Core) - Successes', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy),
     'OTP Submit Success Rate (Core) - Starts', sum(OTP_start_core_full + OTP_start_core_quickPay + OTP_start_core + OTP_start_legacy),
     'OTP Submit Success Rate (Core) - Failures', SUM(otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay + OTP_failure_legacy),
     'OTP Submit Success Rate (Core) - Submits', SUM(OTP_submit_core_full + OTP_submit_core_quickPay + OTP_submit_core + OTP_submit_legacy),
     'OTP Submit Success Rate (Core) - Attempts', sum(OTP_success_core_full + OTP_success_core_quickPay + OTP_success_core + OTP_success_legacy + otp_failure_core + otp_failure_core_full + OTP_failure_core_quickPay),

     'OTP Flow Completion (Mobile) - Successes', SUM(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile),
     'OTP Flow Completion (Mobile) - Starts', SUM(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
     'OTP Flow Completion (Mobile) - Failures', SUM(otp_failure_mobile + otp_failure_mobile_full + OTP_failure_mobile_quickPay),
     'OTP Flow Completion (Mobile) - Submits', SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
     'OTP Flow Completion (Mobile) - Attempts', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),

     'OTP Attempt Success Rate (Mobile) - Successes', SUM(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile),
     'OTP Attempt Success Rate (Mobile) - Starts', SUM(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
     'OTP Attempt Success Rate (Mobile) - Failures', SUM(otp_failure_mobile + otp_failure_mobile_full + OTP_failure_mobile_quickPay),
     'OTP Attempt Success Rate (Mobile) - Submits', SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
     'OTP Attempt Success Rate (Mobile) - Attempts', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),

     'OTP Submit Success Rate (Mobile) - Successes', SUM(OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_success_mobile),
     'OTP Submit Success Rate (Mobile) - Starts', SUM(OTP_start_mobile_full + OTP_start_mobile_quickPay + OTP_start_mobile),
     'OTP Submit Success Rate (Mobile) - Failures', SUM(otp_failure_mobile + otp_failure_mobile_full + OTP_failure_mobile_quickPay),
     'OTP Submit Success Rate (Mobile) - Submits', SUM(OTP_submit_mobile_full + OTP_submit_mobile_quickPay + OTP_submit_mobile),
     'OTP Submit Success Rate (Mobile) - Attempts', SUM(OTP_success_mobile + OTP_success_mobile_full + OTP_success_mobile_quickPay + OTP_failure_mobile + OTP_failure_mobile_full + OTP_failure_mobile_quickPay),

     'Page Views < 3 secs (Home)', sum(page_load_home - page_load_home_hot_3000),
     'Page Views >= 3 secs (Home)', sum(page_load_home_hot_3000),
     'Page Views (Home)', sum(page_load_home),
     'Page Views < 3 secs (Billing)', sum(page_load_billing - page_load_billing_hot_3000),
     'Page Views >= 3 secs (Billing)', sum(page_load_billing_hot_3000),
     'Page Views (Billing)', sum(page_load_billing)

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

      ) AS tmp_map
    FROM
      (
      SELECT
        partition_date_utc,
        visit__application_details__application_name,
        visit__application_details__application_type,
        visit__application_details__app_version,
        agg_custom_visit__account__details__service_subscriptions,
        agg_custom_customer_group,
        agg_visit__account__configuration_factors,
        visit_id,

--------------------------------------------------------------------------------

          SUM(AP_enroll_failure_core) AS AP_enroll_failure_core,
          SUM(AP_enroll_start_core) AS AP_enroll_start_core,
          SUM(AP_enroll_submit_core) AS AP_enroll_submit_core,
          SUM(AP_enroll_success_core) AS AP_enroll_success_core,
          SUM(AP_manage_failure) AS AP_manage_failure,
          SUM(AP_manage_start) AS AP_manage_start,
          SUM(AP_manage_success) AS AP_manage_success,
          SUM(CPNI_cancel) AS CPNI_cancel,
          SUM(CPNI_failure) AS CPNI_failure,
          SUM(CPNI_failure_IMEI) AS CPNI_failure_IMEI,
          SUM(CPNI_failure_email) AS CPNI_failure_email,
          SUM(CPNI_failure_mac_address) AS CPNI_failure_mac_address,
          SUM(CPNI_failure_security_code) AS CPNI_failure_security_code,
          SUM(CPNI_start) AS CPNI_start,
          SUM(CPNI_start_modalView) AS CPNI_start_modalView,
          SUM(CPNI_start_pageView) AS CPNI_start_pageView,
          SUM(CPNI_submit) AS CPNI_submit,
          SUM(CPNI_submit_IMEI) AS CPNI_submit_IMEI,
          SUM(CPNI_submit_email) AS CPNI_submit_email,
          SUM(CPNI_submit_mac_address) AS CPNI_submit_mac_address,
          SUM(CPNI_submit_security_code) AS CPNI_submit_security_code,
          SUM(CPNI_success) AS CPNI_success,
          SUM(CPNI_success_IMEI) AS CPNI_success_IMEI,
          SUM(CPNI_success_email) AS CPNI_success_email,
          SUM(CPNI_success_mac_address) AS CPNI_success_mac_address,
          SUM(CPNI_success_security_code) AS CPNI_success_security_code,
          SUM(OTP_failure_core) AS OTP_failure_core,
          SUM(OTP_failure_core_full) AS OTP_failure_core_full,
          SUM(OTP_failure_core_quickPay) AS OTP_failure_core_quickPay,
          SUM(OTP_failure_legacy) AS OTP_failure_legacy,
          SUM(OTP_failure_mobile) AS OTP_failure_mobile,
          SUM(OTP_failure_mobile_full) AS OTP_failure_mobile_full,
          SUM(OTP_failure_mobile_quickPay) AS OTP_failure_mobile_quickPay,
          SUM(OTP_start_core) AS OTP_start_core,
          SUM(OTP_start_core_full) AS OTP_start_core_full,
          SUM(OTP_start_core_quickPay) AS OTP_start_core_quickPay,
          SUM(OTP_start_legacy) AS OTP_start_legacy,
          SUM(OTP_start_mobile) AS OTP_start_mobile,
          SUM(OTP_start_mobile_full) AS OTP_start_mobile_full,
          SUM(OTP_start_mobile_quickPay) AS OTP_start_mobile_quickPay,
          SUM(OTP_start_modal) AS OTP_start_modal,
          SUM(OTP_submit_core) AS OTP_submit_core,
          SUM(OTP_submit_core_full) AS OTP_submit_core_full,
          SUM(OTP_submit_core_quickPay) AS OTP_submit_core_quickPay,
          SUM(OTP_submit_legacy) AS OTP_submit_legacy,
          SUM(OTP_submit_mobile) AS OTP_submit_mobile,
          SUM(OTP_submit_mobile_full) AS OTP_submit_mobile_full,
          SUM(OTP_submit_mobile_quickPay) AS OTP_submit_mobile_quickPay,
          SUM(OTP_success_core) AS OTP_success_core,
          SUM(OTP_success_core_full) AS OTP_success_core_full,
          SUM(OTP_success_core_quickPay) AS OTP_success_core_quickPay,
          SUM(OTP_success_legacy) AS OTP_success_legacy,
          SUM(OTP_success_mobile) AS OTP_success_mobile,
          SUM(OTP_success_mobile_full) AS OTP_success_mobile_full,
          SUM(OTP_success_mobile_quickPay) AS OTP_success_mobile_quickPay,
          SUM(device_make_payment_failure) AS device_make_payment_failure,
          SUM(device_make_payment_start) AS device_make_payment_start,
          SUM(device_make_payment_submit) AS device_make_payment_submit,
          SUM(device_make_payment_success) AS device_make_payment_success,
          SUM(login_attempt) AS login_attempt,
          SUM(login_failure) AS login_failure,
          SUM(login_success) AS login_success,
          SUM(mobile_activation_activation_complete) AS mobile_activation_activation_complete,
          SUM(mobile_activation_activation_in_progress) AS mobile_activation_activation_in_progress,
          SUM(mobile_activation_transfer_complete) AS mobile_activation_transfer_complete,
          SUM(mobile_activation_transfer_in_progress) AS mobile_activation_transfer_in_progress,
          SUM(mobile_activation_unable_to_activate) AS mobile_activation_unable_to_activate,
          SUM(mobile_activation_unable_to_transfer) AS mobile_activation_unable_to_transfer,
          SUM(mobile_change_plan_start) AS mobile_change_plan_start,
          SUM(mobile_confirm_change_plan) AS mobile_confirm_change_plan,
          SUM(mobile_device_upgrade) AS mobile_device_upgrade,
          SUM(mobile_success_change_plan) AS mobile_success_change_plan,
          SUM(mobile_tier_upgrade_review) AS mobile_tier_upgrade_review,
          SUM(mobile_tier_upgrade_start) AS mobile_tier_upgrade_start,
          SUM(mobile_tier_upgrade_submit) AS mobile_tier_upgrade_submit,
          SUM(mobile_tier_upgrade_success) AS mobile_tier_upgrade_success,
          SUM(page_load_billing) AS page_load_billing,
          SUM(page_load_billing_hot_2000) AS page_load_billing_hot_2000,
          SUM(page_load_billing_hot_3000) AS page_load_billing_hot_3000,
          SUM(page_load_home) AS page_load_home,
          SUM(page_load_home_hot_2000) AS page_load_home_hot_2000,
          SUM(page_load_home_hot_3000) AS page_load_home_hot_3000,
          SUM(payment_method_add_failure_mobile) AS payment_method_add_failure_mobile,
          SUM(payment_method_add_success_mobile) AS payment_method_add_success_mobile,
          SUM(payment_method_delete_failure_mobile) AS payment_method_delete_failure_mobile,
          SUM(payment_method_delete_success_mobile) AS payment_method_delete_success_mobile,
          SUM(payment_method_edit_failure_mobile) AS payment_method_edit_failure_mobile,
          SUM(payment_method_edit_success_mobile) AS payment_method_edit_success_mobile,
          SUM(portals_csat_submit_rating_1_very_dissatisfied) AS portals_csat_submit_rating_1_very_dissatisfied,
          SUM(portals_csat_submit_rating_2_dissatisfied) AS portals_csat_submit_rating_2_dissatisfied,
          SUM(portals_csat_submit_rating_3_neutral) AS portals_csat_submit_rating_3_neutral,
          SUM(portals_csat_submit_rating_4_satisfied) AS portals_csat_submit_rating_4_satisfied,
          SUM(portals_csat_submit_rating_5_very_satisfied) AS portals_csat_submit_rating_5_very_satisfied

FROM `${env:M2MA}`
WHERE (partition_date_utc >= '${env:STARTDATE_2week}' AND partition_date_utc < '${env:END_DATE}')
and AGG_CUSTOM_customer_group = 'Mobile 2.0 Customer'
      GROUP BY
        partition_date_utc,
        visit__application_details__application_name,
        visit__application_details__application_type,
        visit__application_details__app_version,
        agg_custom_visit__account__details__service_subscriptions,
        agg_custom_customer_group,
        agg_visit__account__configuration_factors,
        visit_id
      ) sumfirst
    GROUP BY
      partition_date_utc,
      visit__application_details__application_name,
      visit__application_details__application_type,
      visit__application_details__app_version,
      agg_custom_visit__account__details__service_subscriptions,
      agg_custom_customer_group,
      agg_visit__account__configuration_factors,
      visit_id
 ) boom
 LATERAL VIEW EXPLODE(tmp_map) explode_table AS metric_name, metric_value
 ;

--------------------------------------------------------------------------------
---------------------- ***** DROP TEMPORARY TABLES ***** -----------------------
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS ${env:TMP_db}.calls_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.calls2_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.v_c_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.mvno_accounts_exclude_old_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.mvno_accounts_exclude_new_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.mvno_accounts_exclude_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.mvno_accounts_raw_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.mvno_accounts_agg_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.mvno_counts_${env:CLUSTER} PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.vr_joiner_${env:CLUSTER} PURGE;

--------------------------------------------------------------------------------
------------------------------- ***** END ***** --------------------------------
--------------------------------------------------------------------------------
