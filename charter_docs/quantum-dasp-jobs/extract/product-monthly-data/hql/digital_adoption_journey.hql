USE ${env:DASP_db};

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
insert into asp_digital_monthly_finalagg_${hiveconf:execid}_${hiveconf:stepid}
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
