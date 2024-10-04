USE ${env:DASP_db};

--aggDARTableAcctMonthly
DROP TABLE if EXISTS asp_digital_monthly_aggDARTableAcctMonthly_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_aggDARTableAcctMonthly_${hiveconf:execid}_${hiveconf:stepid} as
SELECT year_month,
       encrypted_account_key_256,
       customerJourney,
       customer_type_distinct,
       if(customer_type_distinct='Residential', DigitalTP_Resi, DigitalTP_SMB) as DigitalTP,
       DigitalTP_SMB,
       DigitalTP_Resi,
       call_counts,
       digital_first_call_counts
from
(SELECT year_month,
       encrypted_account_key_256,
       customerJourney,
       customer_type_distinct,
       sum(distinct_visits_smb) as DigitalTP_SMB,
       sum(distinct_visits_msa + distinct_visits_spec + distinct_visits_specmobile + distinct_visits_selfinstall + distinct_visits_buyflow) as DigitalTP_Resi,
       SUM(call_counts) as call_counts,
       sum(digital_first_call_counts) as digital_first_call_counts
from asp_digital_monthly_finalagg_${hiveconf:execid}_${hiveconf:stepid}
group by year_month,
         encrypted_account_key_256,
         customerJourney,
         customer_type_distinct) a;

--calculateESCMonthly
DROP TABLE if EXISTS asp_digital_monthly_calculateESCMonthly_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_calculateESCMonthly_${hiveconf:execid}_${hiveconf:stepid} as
SELECT customer_type_distinct,
       year_month,
       customerJourney,
       count(distinct if (DigitalTP > 0, encrypted_account_key_256, null)) as DigitalAdoption,
       count(distinct if ((DigitalTP + call_counts)= 0, encrypted_account_key_256, null)) as NoTouchPoints,
       count(distinct if ((DigitalTP = 0 and call_counts >0), encrypted_account_key_256, null)) as CallOnly,
       count(distinct if ((DigitalTP > 0 and call_counts =0), encrypted_account_key_256, null)) as DigitalOnly,
       count(distinct if ((DigitalTP > 0 and call_counts >0), encrypted_account_key_256, null)) as DigitalAndCall,
       sum(call_counts) as call_counts,
       SUM(digital_first_call_counts) as digital_first_call_counts
from asp_digital_monthly_aggDARTableAcctMonthly_${hiveconf:execid}_${hiveconf:stepid}
where customerJourney is not null
group by year_month,
         customerJourney,
         customer_type_distinct;

--finalESCMonthly
DROP TABLE if EXISTS asp_digital_monthly_finalESCMonthly_${hiveconf:execid}_${hiveconf:stepid} PURGE;
create table asp_digital_monthly_finalESCMonthly_${hiveconf:execid}_${hiveconf:stepid} AS
select *
from asp_digital_monthly_overrall_${hiveconf:execid}_${hiveconf:stepid}
UNION
SELECT *
from asp_digital_monthly_calculateESCMonthly_${hiveconf:execid}_${hiveconf:stepid};

INSERT into TABLE asp_digital_adoption_monthly_archive PARTITION(run_date)
select a.customer_type_distinct,
       customerJourney,
       total_custs,
       (callonly + digitalonly + digitalandcall) as supportSeeking_HH,
       digitaladoption as digitalTP_HH,
       digitalonly as digitalOnlyTP_HH,
       call_counts,
       digital_first_call_counts,
       digitaladoption/(callonly + digitalonly + digitalandcall) as digitalAwarenessRatio,
       digitalonly/digitaladoption as digitalEffectivenessRatio,
       call_counts/(callonly + digitalonly + digitalandcall) as callsPerEngagedHH,
       digital_first_call_counts/(callonly + digitalonly + digitalandcall) as digitalFirstCallsPerEngagedHH,
       (call_counts/(callonly + digitalonly + digitalandcall) - digital_first_call_counts/(callonly + digitalonly + digitalandcall)) as nonDigitalFirstCallsPerEngagedHH,
       digital_first_call_counts/call_counts as shareDigitalFirstCalls,
       a.year_month,
       current_date
from asp_digital_monthly_finalESCMonthly_${hiveconf:execid}_${hiveconf:stepid} a
left join asp_digital_monthly_overrall_total_customers_${hiveconf:execid}_${hiveconf:stepid} b
on a.year_month = b.year_month
and a.customer_type_distinct = b.customer_type_distinct
order by a.year_month, a.customer_type_distinct, customerJourney;

INSERT overwrite TABLE asp_digital_adoption_monthly PARTITION(year_month)
select a.customer_type_distinct,
       customerJourney,
       total_custs,
       (callonly + digitalonly + digitalandcall) as supportSeeking_HH,
       digitaladoption as digitalTP_HH,
       digitalonly as digitalOnlyTP_HH,
       call_counts,
       digital_first_call_counts,
       digitaladoption/(callonly + digitalonly + digitalandcall) as digitalAwarenessRatio,
       digitalonly/digitaladoption as digitalEffectivenessRatio,
       call_counts/(callonly + digitalonly + digitalandcall) as callsPerEngagedHH,
       digital_first_call_counts/(callonly + digitalonly + digitalandcall) as digitalFirstCallsPerEngagedHH,
       (call_counts/(callonly + digitalonly + digitalandcall) - digital_first_call_counts/(callonly + digitalonly + digitalandcall)) as nonDigitalFirstCallsPerEngagedHH,
       digital_first_call_counts/call_counts as shareDigitalFirstCalls,
       a.year_month
from asp_digital_monthly_finalESCMonthly_${hiveconf:execid}_${hiveconf:stepid} a
left join asp_digital_monthly_overrall_total_customers_${hiveconf:execid}_${hiveconf:stepid} b
on a.year_month = b.year_month
and a.customer_type_distinct = b.customer_type_distinct
order by a.year_month, a.customer_type_distinct, customerJourney;

DROP TABLE IF EXISTS asp_digital_monthly_raw_overrall_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_overrall_total_customers_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_overrall_acct_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_overrall_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_rawdar_tmp_1_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_rawdar_tmp_2_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_rawdar_tmp_3_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_rawdar_tmp_4_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_rawdar_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_raw_journey_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_calljourney_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_digFirstCallJourney_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_digTPJourney_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_finalagg_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_aggDARTableAcctMonthly_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_calculateESCMonthly_${hiveconf:execid}_${hiveconf:stepid} PURGE;
DROP TABLE if EXISTS asp_digital_monthly_finalESCMonthly_${hiveconf:execid}_${hiveconf:stepid} PURGE;
