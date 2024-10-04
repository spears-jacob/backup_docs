USE ${env:ENVIRONMENT};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.tez.container.size=16000;

set hive.auto.convert.join=false;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.input.fileinputformat.split.minsize=524288000;
set mapreduce.input.fileinputformat.split.maxsize=524288000;
set hive.optimize.sort.dynamic.partition = false;

INSERT OVERWRITE TABLE asp_idm_metric_agg PARTITION (denver_date)

-- Dimensions go here
select
  state__view__current_page__page_name AS page_name,
  state__view__current_page__app_section as app_section,
  visit__user__role as user_role,
  visit__device__enc_uuid as device_id,
  visit__visit_id as visit_id,
  visit__application_details__application_type AS application_type,
  LOWER(visit__device__device_type) AS device_type,
  visit__application_details__app_version AS app_version,
  visit__login__logged_in AS logged_in,
  LOWER(visit__application_details__application_name) AS application_name,
  'All OS Names' AS os_name,
  visit__device__operating_system AS operating_system,
  visit__device__browser__name AS browser_name,
  visit__device__browser__version AS browser_version,
  state__view__current_page__ui_responsive_breakpoint as browser_size_breakpoint,
  visit__device__device_form_factor AS form_factor,
  parse_url(visit__application_details__referrer_link,'HOST') AS referrer_link,

-------------------------------------------------
--- Begin Rosetta-Generated Metric Defintions ---
---------------- IDM Metric Agg -----------------
-------------------------------------------------
