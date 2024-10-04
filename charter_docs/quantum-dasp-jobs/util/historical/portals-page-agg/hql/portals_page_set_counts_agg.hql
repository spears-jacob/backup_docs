USE ${env:DASP_db};

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join = true;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;
SET hive.exec.dynamic.partition.mode=nonstrict;

SET hive.merge.tezfiles=true;
set hive.merge.smallfiles.avgsize=1024000000;
set hive.merge.size.per.task=1024000000;

--instances
INSERT OVERWRITE TABLE ${env:DASP_db}.asp_page_set_counts_agg PARTITION (denver_date, unit_type)
SELECT
      application_name,
      current_page_name,
      current_article_name,
      standardized_name,
      modal_name,
      modal_view_count,
      page_view_count,
      select_action_count,
      spinner_success_count,
      spinner_failure_count,
      toggle_flips_count,
      grouping_id,
      denver_date,
      unit_type
  FROM (
   SELECT
         application_name,
         current_page_name,
         current_article_name,
         standardized_name,
         modal_name,
         SUM(modal_view_instances)       AS modal_view_count,
         SUM(page_view_instances)        AS page_view_count,
         SUM(select_action_instances)    AS select_action_count,
         SUM(spinner_success_instances)  AS spinner_success_count,
         SUM(spinner_failure_instances)  AS spinner_failure_count,
         SUM(toggle_flip_instances)      AS toggle_flips_count,
         CAST(grouping__id AS INT)       AS grouping_id,
         denver_date,
         'instances'                     AS unit_type
     FROM ${env:DASP_db}.asp_page_agg
    WHERE denver_date                    >= '${hiveconf:START_DATE}'
      AND denver_date                    <  '${hiveconf:END_DATE}'
   GROUP BY
       denver_date,
       application_name,
       current_page_name,
       current_article_name,
       standardized_name,
       modal_name
   GROUPING SETS (
      (denver_date, application_name, current_page_name),
      (denver_date, application_name, current_page_name, current_article_name, standardized_name, modal_name))
   ) sets
;

--devices
INSERT OVERWRITE TABLE ${env:DASP_db}.asp_page_set_counts_agg PARTITION (denver_date, unit_type)
SELECT
      application_name,
      current_page_name,
      current_article_name,
      standardized_name,
      modal_name,
      modal_view_count,
      page_view_count,
      select_action_count,
      spinner_success_count,
      spinner_failure_count,
      toggle_flips_count,
      grouping_id,
      denver_date,
      unit_type
  FROM (
   SELECT
         application_name,
         current_page_name,
         current_article_name,
         standardized_name,
         modal_name,
         SIZE(COLLECT_SET(IF(modal_view_devices > 0, device_id, NULL)))       AS modal_view_count,
         SIZE(COLLECT_SET(IF(page_view_devices > 0, device_id, NULL)))        AS page_view_count,
         SIZE(COLLECT_SET(IF(select_action_devices > 0, device_id, NULL)))    AS select_action_count,
         SIZE(COLLECT_SET(IF(spinner_success_devices > 0, device_id, NULL)))  AS spinner_success_count,
         SIZE(COLLECT_SET(IF(spinner_failure_devices > 0, device_id, NULL)))  AS spinner_failure_count,
         SIZE(COLLECT_SET(IF(toggle_flip_devices > 0, device_id, NULL)))      AS toggle_flips_count,
         CAST(grouping__id AS INT)       AS grouping_id,
         denver_date,
         'devices'                       AS unit_type
     FROM ${env:DASP_db}.asp_page_agg
    WHERE denver_date                    >= '${hiveconf:START_DATE}'
      AND denver_date                    <  '${hiveconf:END_DATE}'
   GROUP BY
       denver_date,
       application_name,
       current_page_name,
       current_article_name,
       standardized_name,
       modal_name
   GROUPING SETS (
      (denver_date, application_name, current_page_name),
      (denver_date, application_name, current_page_name, current_article_name, standardized_name, modal_name))
   ) sets
;

--households
INSERT OVERWRITE TABLE ${env:DASP_db}.asp_page_set_counts_agg PARTITION (denver_date, unit_type)
SELECT
      application_name,
      current_page_name,
      current_article_name,
      standardized_name,
      modal_name,
      modal_view_count,
      page_view_count,
      select_action_count,
      spinner_success_count,
      spinner_failure_count,
      toggle_flips_count,
      grouping_id,
      denver_date,
      unit_type
  FROM (
   SELECT
         application_name,
         current_page_name,
         current_article_name,
         standardized_name,
         modal_name,
         SIZE(COLLECT_SET(IF(modal_view_households > 0, account_number, NULL)))        AS modal_view_count,
         SIZE(COLLECT_SET(IF(page_view_households > 0, account_number, NULL)))         AS page_view_count,
         SIZE(COLLECT_SET(IF(select_action_households > 0, account_number, NULL)))     AS select_action_count,
         SIZE(COLLECT_SET(IF(spinner_success_households > 0, account_number, NULL)))   AS spinner_success_count,
         SIZE(COLLECT_SET(IF(spinner_failure_households > 0, account_number, NULL)))   AS spinner_failure_count,
         SIZE(COLLECT_SET(IF(toggle_flip_households > 0, account_number, NULL)))       AS toggle_flips_count,
         CAST(grouping__id AS INT)       AS grouping_id,
         denver_date,
         'households'                    AS unit_type
     FROM ${env:DASP_db}.asp_page_agg
    WHERE denver_date                    >= '${hiveconf:START_DATE}'
      AND denver_date                    <  '${hiveconf:END_DATE}'
   GROUP BY
       denver_date,
       application_name,
       current_page_name,
       current_article_name,
       standardized_name,
       modal_name
   GROUPING SETS (
      (denver_date, application_name, current_page_name),
      (denver_date, application_name, current_page_name, current_article_name, standardized_name, modal_name))
   ) sets
;

--visits
INSERT OVERWRITE TABLE ${env:DASP_db}.asp_page_set_counts_agg PARTITION (denver_date, unit_type)
SELECT
      application_name,
      current_page_name,
      current_article_name,
      standardized_name,
      modal_name,
      modal_view_count,
      page_view_count,
      select_action_count,
      spinner_success_count,
      spinner_failure_count,
      toggle_flips_count,
      grouping_id,
      denver_date,
      unit_type
  FROM (
   SELECT
         application_name,
         current_page_name,
         current_article_name,
         standardized_name,
         modal_name,
         SIZE(COLLECT_SET(IF(modal_view_visits > 0, visit_id, NULL)))        AS modal_view_count,
         SIZE(COLLECT_SET(IF(page_view_visits > 0, visit_id, NULL)))         AS page_view_count,
         SIZE(COLLECT_SET(IF(select_action_visits > 0, visit_id, NULL)))     AS select_action_count,
         SIZE(COLLECT_SET(IF(spinner_success_visits > 0, visit_id, NULL)))   AS spinner_success_count,
         SIZE(COLLECT_SET(IF(spinner_failure_visits > 0, visit_id, NULL)))   AS spinner_failure_count,
         SIZE(COLLECT_SET(IF(toggle_flip_visits > 0, visit_id, NULL)))       AS toggle_flips_count,
         CAST(grouping__id AS INT)       AS grouping_id,
         denver_date,
         'visits'                        AS unit_type
     FROM ${env:DASP_db}.asp_page_agg
    WHERE denver_date                    >= '${hiveconf:START_DATE}'
      AND denver_date                    <  '${hiveconf:END_DATE}'
   GROUP BY
       denver_date,
       application_name,
       current_page_name,
       current_article_name,
       standardized_name,
       modal_name
   GROUPING SETS (
      (denver_date, application_name, current_page_name),
      (denver_date, application_name, current_page_name, current_article_name, standardized_name, modal_name))
   ) sets
;
