USE ${env:ENVIRONMENT};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.tez.container.size=16000;
set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.maxsize=268435456;
set mapreduce.input.fileinputformat.split.minsize=268435456;
set hive.optimize.sort.dynamic.partition = false;

SELECT '
--------------------------------------------------------------------------------
---- *** 01: Gather "Base" Calls *** -------------------------------------------
--------------------------------------------------------------------------------
'
;

CREATE TEMPORARY TABLE rcr_calls_base as
select
  call_inbound_key,
  call_start_timestamp_utc,
  call_end_timestamp_utc,
  segment_start_timestamp_utc,
  encrypted_account_number_256,
  customer_type,
  account_agent_mso,
  product,
  location_name,
  call_end_date_utc,
  prod.aes_decrypt256(encrypted_created_by_256) as created_by,
  ROW_NUMBER() OVER(PARTITION BY call_end_date_utc,account_agent_mso,encrypted_account_number_256,call_inbound_key,product ORDER BY segment_start_timestamp_utc ASC) as seg_num_asc
from prod.red_cs_call_care_data_v
WHERE call_end_date_utc between DATE_ADD('${env:END_DATE}',-14) and '${env:END_DATE}'
  and encrypted_account_number_256 not in ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==','7TTmD9UkKwdYsmcWS5tPUQ==')
  and segment_handled_flag = 1
  and product <> 'Unknown'
;

SELECT '
--------------------------------------------------------------------------------
---- *** 02: Base Calls (Earliest Timestamp) *** -------------------------------
--------------------------------------------------------------------------------
'
;

CREATE TEMPORARY TABLE rcr_calls_base_1 as
select
  call_inbound_key,
  call_start_timestamp_utc,
  call_end_timestamp_utc,
  segment_start_timestamp_utc,
  encrypted_account_number_256,
  customer_type,
  account_agent_mso,
  product,
  location_name,
  call_end_date_utc,
  created_by,
  seg_num_asc
from rcr_calls_base
WHERE seg_num_asc = 1
;

SELECT '
--------------------------------------------------------------------------------
---- *** 03: 7 days prior "After" calls *** ------------------------------------
--------------------------------------------------------------------------------
'
;

CREATE TEMPORARY TABLE rcr_calls_aft as
select
  call_inbound_key,
  call_start_timestamp_utc,
  call_end_timestamp_utc,
  segment_start_timestamp_utc,
  encrypted_account_number_256,
  account_agent_mso,
  product,
  call_end_date_utc
from prod.red_cs_call_care_data_v
WHERE call_end_date_utc between DATE_ADD('${env:END_DATE}',-14) and DATE_ADD('${env:END_DATE}',-7)
  and encrypted_account_number_256 not in ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==','7TTmD9UkKwdYsmcWS5tPUQ==')
  and segment_handled_flag = 1
  and product <> 'Unknown'
;

SELECT '
--------------------------------------------------------------------------------
---- *** 04: 7 days prior "Before" calls *** -----------------------------------
--------------------------------------------------------------------------------
'
;

CREATE TEMPORARY TABLE rcr_calls_bef as
select
  call_inbound_key,
  call_start_timestamp_utc,
  call_end_timestamp_utc,
  segment_start_timestamp_utc,
  encrypted_account_number_256,
  account_agent_mso,
  product,
  call_end_date_utc
from prod.red_cs_call_care_data_v
WHERE call_end_date_utc between DATE_ADD('${env:END_DATE}',-7) and '${env:END_DATE}'
  and encrypted_account_number_256 not in ('7FbKtybuOWU4/Q0SRInbHA==','GSWNkZXIfDPD6x25Na3i8g==','7TTmD9UkKwdYsmcWS5tPUQ==')
  and segment_handled_flag = 1
  and product <> 'Unknown'
;

SELECT '
--------------------------------------------------------------------------------
---- *** 05: Aggregate "After" Call Numbers *** --------------------------------
--------------------------------------------------------------------------------
'
;

CREATE TEMPORARY TABLE rcr_aft_agg as
select
  base.call_inbound_key,
  base.encrypted_account_number_256,
  base.account_agent_mso,
  base.product,
  base.call_end_date_utc,
  size(collect_set(if(aft.call_start_timestamp_utc - base.call_end_timestamp_utc <= 604800000 and aft.call_start_timestamp_utc - base.call_end_timestamp_utc > 0 and base.product=aft.product,aft.call_inbound_key,null))) as num_calls_product,
  size(collect_set(if(aft.call_start_timestamp_utc - base.call_end_timestamp_utc <= 604800000 and aft.call_start_timestamp_utc - base.call_end_timestamp_utc > 0 and aft.product='Retention',aft.call_inbound_key,null))) as num_calls_retention
from rcr_calls_base_1 base
  left join rcr_calls_aft aft on base.encrypted_account_number_256 = aft.encrypted_account_number_256
    and base.account_agent_mso = aft.account_agent_mso
group by
base.call_inbound_key,
base.encrypted_account_number_256,
base.account_agent_mso,
base.product,
base.call_end_date_utc
;

SELECT '
--------------------------------------------------------------------------------
---- *** 06: Aggregate "Before" Call Numbers *** -------------------------------
--------------------------------------------------------------------------------
'
;

CREATE TEMPORARY TABLE rcr_bef_agg as
select
  base.call_inbound_key,
  base.encrypted_account_number_256,
  base.account_agent_mso,
  base.product,
  base.call_end_date_utc,
  size(collect_set(if(base.call_start_timestamp_utc - bef.call_end_timestamp_utc <= 604800000 and base.call_start_timestamp_utc - bef.call_end_timestamp_utc > 0 and base.product=bef.product,bef.call_inbound_key,null))) as num_calls_product,
  size(collect_set(if(base.call_start_timestamp_utc - bef.call_end_timestamp_utc <= 604800000 and base.call_start_timestamp_utc - bef.call_end_timestamp_utc > 0 and bef.product='Retention',bef.call_inbound_key,null))) as num_calls_retention
from rcr_calls_base_1 base
  left join rcr_calls_bef bef on base.encrypted_account_number_256 = bef.encrypted_account_number_256
    and base.account_agent_mso = bef.account_agent_mso
group by
base.call_inbound_key,
base.encrypted_account_number_256,
base.account_agent_mso,
base.product,
base.call_end_date_utc
;

SELECT '
--------------------------------------------------------------------------------
---- *** 07: Call Data Portion *** ---------------------------------------------
--------------------------------------------------------------------------------
'
;

INSERT OVERWRITE TABLE rcr_calls_analysis PARTITION (call_end_date_utc)
select
  base.call_inbound_key,
  base.encrypted_account_number_256,
  base.customer_type,
  base.account_agent_mso,
  base.product,
  base.location_name,
  base.created_by,
  bef.num_calls_product as bef_num_calls_product,
  bef.num_calls_retention as bef_num_calls_retention,
  aft.num_calls_product as aft_num_calls_product,
  aft.num_calls_retention as aft_num_calls_retention,
  base.call_end_date_utc
from rcr_calls_base_1 base
  left join rcr_bef_agg bef on base.call_inbound_key = bef.call_inbound_key
    and base.account_agent_mso = bef.account_agent_mso
    and base.product = bef.product
  left join rcr_aft_agg aft on base.call_inbound_key = aft.call_inbound_key
    and base.account_agent_mso = aft.account_agent_mso
    and base.product = aft.product
;

SELECT '
--------------------------------------------------------------------------------
---- *** 08: Purge Temporary Tables *** ----------------------------------------
--------------------------------------------------------------------------------
'
;

drop table if exists rcr_calls_base PURGE;
drop table if exists rcr_calls_base_1 PURGE;
drop table if exists rcr_calls_aft PURGE;
drop table if exists rcr_calls_bef PURGE;
drop table if exists rcr_aft_agg PURGE;
drop table if exists rcr_bef_agg PURGE;
