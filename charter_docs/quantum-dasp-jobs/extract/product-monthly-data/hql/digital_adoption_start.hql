USE ${env:DASP_db};

MSCK REPAIR TABLE asp_digital_adoption_daily;

ADD JAR ${env:SCALA_LIB};
ADD JAR s3://pi-global-${env:ENVIRONMENT}-udf-jars/daspscalaudf_2.11-0.1.jar;
CREATE TEMPORARY FUNCTION udf_remove_dups AS 'com.charter.dasp.udf.DupRemover';
CREATE TEMPORARY FUNCTION udf_get_keys AS 'com.charter.dasp.udf.MapKey';
CREATE TEMPORARY FUNCTION udf_get_map_value AS 'com.charter.dasp.udf.MapValue';
CREATE TEMPORARY FUNCTION udf_merge_arrays AS 'com.charter.dasp.udf.ArrayMerger';

--getRawDARTableOverall
DROP TABLE if EXISTS asp_digital_monthly_raw_overrall_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_raw_overrall_${hiveconf:execid}_${hiveconf:stepid} as
   SELECT encrypted_account_key_256,
          substr(partition_date_denver,0,7) as year_month,
          legacy_company,
          udf_remove_dups(customer_type_list) as customer_type_distinct,
          if (distinct_visits_smb is Null, 0, 1) as distinct_visits_smb,
          if (distinct_visits_msa is Null, 0, 1) as distinct_visits_msa,
          if (distinct_Visits_spec is Null, 0, 1) as distinct_visits_spec,
          if (distinct_visits_specmobile is Null, 0, 1) as distinct_visits_specmobile,
          if (distinct_visits_selfinstall is Null, 0, 1) as distinct_visits_selfinstall,
          if (distinct_visits_buyflow is Null, 0, 1) as distinct_visits_buyflow,
          if (has_recurring_payment, 1, 0) has_recurring_payment,
          IF (call_counts is null, 0, call_counts) as call_counts,
          if (digital_first_call_counts is Null, 0, digital_first_call_counts) as digital_first_call_counts
    from asp_digital_adoption_daily
   where partition_date_denver between '${hiveconf:START_DATE}' and DATE_ADD('${hiveconf:END_DATE}',-1)
;

--getTotalCustomersMonthly
DROP TABLE if EXISTS asp_digital_monthly_overrall_total_customers_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_overrall_total_customers_${hiveconf:execid}_${hiveconf:stepid} as
select customer_type_distinct,
       year_month,
       count(distinct encrypted_account_key_256) as total_custs
from asp_digital_monthly_raw_overrall_${hiveconf:execid}_${hiveconf:stepid}
group by customer_type_distinct, year_month
ORDER by customer_type_distinct, year_month;

--aggDARTableAcctMonthlyOverall
DROP TABLE if EXISTS asp_digital_monthly_overrall_acct_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_overrall_acct_${hiveconf:execid}_${hiveconf:stepid} as
select customer_type_distinct,
       year_month,
       encrypted_account_key_256,
       has_recurring_payment,
       DigitalTP_SMB,
       DigitalTP_Resi,
       if(customer_type_distinct='Residential', DigitalTP_Resi, DigitalTP_SMB) as DigitalTP,
       call_counts,
       digital_first_call_counts
FROM
(select customer_type_distinct,
       year_month,
       encrypted_account_key_256,
       sum(has_recurring_payment) as has_recurring_payment,
       sum(distinct_visits_smb) as DigitalTP_SMB,
       SUM(distinct_visits_msa + distinct_visits_spec + distinct_visits_specmobile + distinct_visits_selfinstall + distinct_visits_buyflow) as DigitalTP_Resi,
       SUM(call_counts) as call_counts,
       SUM(digital_first_call_counts) as digital_first_call_counts
from asp_digital_monthly_raw_overrall_${hiveconf:execid}_${hiveconf:stepid}
group by customer_type_distinct, year_month, encrypted_account_key_256) a;

--calculateESCMonthlyOverall
DROP TABLE if EXISTS asp_digital_monthly_overrall_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_overrall_${hiveconf:execid}_${hiveconf:stepid} as
SELECT customer_type_distinct,
       year_month,
       'Overall' as customerJourney,
       count(distinct if (DigitalTP > 0, encrypted_account_key_256, null)) as DigitalAdoption,
       count(distinct if ((DigitalTP + call_counts)= 0, encrypted_account_key_256, null)) as NoTouchPoints,
       count(distinct if ((DigitalTP = 0 and call_counts >0), encrypted_account_key_256, null)) as CallOnly,
       count(distinct if ((DigitalTP > 0 and call_counts =0), encrypted_account_key_256, null)) as DigitalOnly,
       count(distinct if ((DigitalTP > 0 and call_counts >0), encrypted_account_key_256, null)) as DigitalAndCall,
       sum(call_counts) as call_counts,
       sum(digital_first_call_counts) as digital_first_call_counts
from asp_digital_monthly_overrall_acct_${hiveconf:execid}_${hiveconf:stepid}
group by customer_type_distinct, year_month;

--First step of getRawDARTable
DROP TABLE if EXISTS asp_digital_monthly_rawdar_tmp_1_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_rawdar_tmp_1_${hiveconf:execid}_${hiveconf:stepid} as
SELECT encrypted_account_key_256,
       partition_date_denver,
       legacy_company,
       digital_first_journey_call_map,
       customer_journey_calls_map,
       substr(partition_date_denver,0,7) as year_month,
       udf_remove_dups(customer_type_list) as customer_type_distinct,
       if (distinct_visits_smb is Null, 0, 1) as distinct_visits_smb,
       if (distinct_visits_msa is Null, 0, 1) as distinct_visits_msa,
       if (distinct_Visits_spec is Null, 0, 1) as distinct_Visits_spec,
       if (distinct_visits_specmobile is Null, 0, 1) as distinct_visits_specmobile,
       if (distinct_visits_selfinstall is Null, 0, 1) as distinct_visits_selfinstall,
       if (distinct_visits_buyflow is Null, 0, 1) as distinct_visits_buyflow,
       if (has_recurring_payment, 1, 0) has_recurring_payment,
       IF (call_counts is null, 0, call_counts) as call_counts,
       if (digital_first_call_counts is Null, 0, digital_first_call_counts) as digital_first_call_counts,

       if (customer_journey_calls_map is Null, "No Journey", udf_get_keys(customer_journey_calls_map)) as cust_journey_distinct,
       if (customer_visit_journey_spec is Null, "No Journey", udf_get_keys(customer_visit_journey_spec)) as cust_journey_distinct_spec,
       IF (customer_visit_journey_msa is Null, "No Journey", udf_get_keys(customer_visit_journey_msa)) as cust_journey_distinct_msa,
       if (customer_visit_journey_selfinstall is Null, "No Journey", udf_get_keys(customer_visit_journey_selfinstall)) as cust_journey_distinct_selfinstall,
       if (customer_visit_journey_buyflow is Null, "No Journey", udf_get_keys(customer_visit_journey_buyflow)) as cust_journey_distinct_buyflow,
       if (customer_visit_journey_smb is Null, "No Journey", udf_get_keys(customer_visit_journey_smb)) as digital_tp_cust_journey_list_smb,
       IF (digital_first_journey_call_map is Null, "No Journey", udf_get_keys(digital_first_journey_call_map)) as cust_journey_distinct_dig1st
  from asp_digital_adoption_daily
 where partition_date_denver between '${hiveconf:START_DATE}' and DATE_ADD('${hiveconf:END_DATE}',-1)
;

DROP TABLE if EXISTS asp_digital_monthly_rawdar_tmp_2_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_rawdar_tmp_2_${hiveconf:execid}_${hiveconf:stepid} as
   SELECT *,
          udf_merge_arrays(udf_merge_arrays(udf_merge_arrays(
          cust_journey_distinct_spec,
          cust_journey_distinct_msa),
          cust_journey_distinct_selfinstall),
          cust_journey_distinct_buyflow) as digital_tp_cust_journey_list_resi
    from asp_digital_monthly_rawdar_tmp_1_${hiveconf:execid}_${hiveconf:stepid}
;

DROP TABLE if EXISTS asp_digital_monthly_rawdar_tmp_3_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_rawdar_tmp_3_${hiveconf:execid}_${hiveconf:stepid} as
   SELECT *,
          CASE when (customer_type_distinct='Residential') THEN digital_tp_cust_journey_list_resi
               when(customer_type_distinct='Commercial') THEN digital_tp_cust_journey_list_smb
          END as digital_tp_cust_journey_list
    from asp_digital_monthly_rawdar_tmp_2_${hiveconf:execid}_${hiveconf:stepid}
;

DROP TABLE if EXISTS asp_digital_monthly_rawdar_tmp_4_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_rawdar_tmp_4_${hiveconf:execid}_${hiveconf:stepid} as
      SELECT *
        from asp_digital_monthly_rawdar_tmp_3_${hiveconf:execid}_${hiveconf:stepid}
        lateral view explode(split(cust_journey_distinct,",")) exploded_table as call_cust_journeys_exp
        lateral view explode(split(digital_tp_cust_journey_list,",")) exploded_table as dig_tp_cust_journeys_exp
        lateral view explode(split(cust_journey_distinct_dig1st,",")) exploded_table as digital_first_call_cust_journeys_exp
;

--Final step of getRawDARTable
DROP TABLE if EXISTS asp_digital_monthly_rawdar_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_rawdar_${hiveconf:execid}_${hiveconf:stepid} as
   SELECT *,
          CASE when(customer_journey_calls_map is Not Null) THEN
              udf_get_map_value(customer_journey_calls_map, TRIM(call_cust_journeys_exp))
          END as journey_call_counts,
          CASE when(digital_first_journey_call_map is Not Null) THEN
              udf_get_map_value(digital_first_journey_call_map, TRIM(digital_first_call_cust_journeys_exp))
          END as journey_digital_first_call_counts
    from asp_digital_monthly_rawdar_tmp_4_${hiveconf:execid}_${hiveconf:stepid}
;

--rawData journey
DROP TABLE if EXISTS asp_digital_monthly_raw_journey_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_raw_journey_${hiveconf:execid}_${hiveconf:stepid} as
SELECT *,
       '${hiveconf:JOURNEY}' as customerJourney
from asp_digital_monthly_rawdar_${hiveconf:execid}_${hiveconf:stepid}
WHERE TRIM(call_cust_journeys_exp) in ('${hiveconf:JOURNEY}')
or TRIM(digital_first_call_cust_journeys_exp) in ('${hiveconf:JOURNEY}')
or TRIM(dig_tp_cust_journeys_exp) in ('${hiveconf:JOURNEY}')
;

--rawData callJourney
DROP TABLE if EXISTS asp_digital_monthly_calljourney_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_calljourney_${hiveconf:execid}_${hiveconf:stepid} as
SELECT encrypted_account_key_256,
       partition_date_denver,
       year_month,
       TRIM(call_cust_journeys_exp) as customerJourney,
       customer_type_distinct,
       max(if(journey_call_counts is null, 0, journey_call_counts)) as call_counts
from asp_digital_monthly_raw_journey_${hiveconf:execid}_${hiveconf:stepid}
where TRIM(call_cust_journeys_exp) in ('${hiveconf:JOURNEY}')
group by partition_date_denver,
         year_month,
         encrypted_account_key_256,
         TRIM(call_cust_journeys_exp),
         customer_type_distinct;

--rawData digFirstCallJourney
DROP TABLE if EXISTS asp_digital_monthly_digFirstCallJourney_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_digFirstCallJourney_${hiveconf:execid}_${hiveconf:stepid} as
SELECT encrypted_account_key_256,
       partition_date_denver,
       year_month,
       TRIM(digital_first_call_cust_journeys_exp) as customerJourney,
       customer_type_distinct,
       max(if(journey_digital_first_call_counts is null, 0, journey_digital_first_call_counts)) as digital_first_call_counts
from asp_digital_monthly_raw_journey_${hiveconf:execid}_${hiveconf:stepid}
where TRIM(digital_first_call_cust_journeys_exp) in ('${hiveconf:JOURNEY}')
group by partition_date_denver,
         year_month,
         encrypted_account_key_256,
         TRIM(digital_first_call_cust_journeys_exp),
         customer_type_distinct;

--rawData digTPJourney
DROP TABLE if EXISTS asp_digital_monthly_digTPJourney_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_digTPJourney_${hiveconf:execid}_${hiveconf:stepid} as
SELECT encrypted_account_key_256,
       partition_date_denver,
       year_month,
       TRIM(dig_tp_cust_journeys_exp) as customerJourney,
       customer_type_distinct,
       max(distinct_visits_smb) as distinct_visits_smb,
       max(distinct_visits_msa) as distinct_visits_msa,
       max(distinct_visits_spec) as distinct_visits_spec,
       max(distinct_visits_specmobile) as distinct_visits_specmobile,
       max(distinct_visits_selfinstall) as distinct_visits_selfinstall,
       max(distinct_visits_buyflow) as distinct_visits_buyflow
from asp_digital_monthly_raw_journey_${hiveconf:execid}_${hiveconf:stepid}
where TRIM(dig_tp_cust_journeys_exp) in ('${hiveconf:JOURNEY}')
group by partition_date_denver,
         year_month,
         encrypted_account_key_256,
         TRIM(dig_tp_cust_journeys_exp),
         customer_type_distinct;

--Final Agg
DROP TABLE if EXISTS asp_digital_monthly_finalagg_${hiveconf:execid}_${hiveconf:stepid} PURGE;
CREATE TABLE asp_digital_monthly_finalagg_${hiveconf:execid}_${hiveconf:stepid} as
select distinct a.encrypted_account_key_256,
       a.partition_date_denver,
       a.year_month,
       a.customerJourney,
       a.customer_type_distinct,
       if(b.call_counts is null, 0, b.call_counts) as call_counts,
       if(c.digital_first_call_counts is null, 0, c.digital_first_call_counts) as digital_first_call_counts,
       if(d.distinct_visits_smb is null, 0, 1) as distinct_visits_smb,
       if(d.distinct_visits_msa is null, 0, 1) as distinct_visits_msa,
       if(d.distinct_visits_spec is null, 0, 1) as distinct_visits_spec,
       if(d.distinct_visits_specmobile is null, 0, 1) as distinct_visits_specmobile,
       if(d.distinct_visits_selfinstall is null, 0, 1) as distinct_visits_selfinstall,
       if(d.distinct_visits_buyflow is null, 0, 1) as distinct_visits_buyflow
  from asp_digital_monthly_raw_journey_${hiveconf:execid}_${hiveconf:stepid} a
  left join asp_digital_monthly_calljourney_${hiveconf:execid}_${hiveconf:stepid} b
  on a.encrypted_account_key_256=b.encrypted_account_key_256
  and a.partition_date_denver=b.partition_date_denver
  and a.year_month=b.year_month
  and a.customerJourney = b.customerJourney
  AND a.customer_type_distinct = b.customer_type_distinct
  Left join asp_digital_monthly_digFirstCallJourney_${hiveconf:execid}_${hiveconf:stepid} c
  on a.encrypted_account_key_256=c.encrypted_account_key_256
  and a.partition_date_denver=c.partition_date_denver
  and a.year_month=c.year_month
  and a.customerJourney = c.customerJourney
  AND a.customer_type_distinct = c.customer_type_distinct
  Left join asp_digital_monthly_digTPJourney_${hiveconf:execid}_${hiveconf:stepid} d
  on a.encrypted_account_key_256=d.encrypted_account_key_256
  and a.partition_date_denver=d.partition_date_denver
  and a.year_month=d.year_month
  and a.customerJourney = d.customerJourney
  AND a.customer_type_distinct = d.customer_type_distinct;
