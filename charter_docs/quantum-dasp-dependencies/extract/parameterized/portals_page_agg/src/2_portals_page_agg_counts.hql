set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;
set hive.auto.convert.join = true;

set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.optimize.sort.dynamic.partition = false;

USE ${env:ENVIRONMENT};

INSERT OVERWRITE TABLE asp_page_agg_counts PARTITION (denver_date, unit_type)
SELECT
     application_name,
     current_page_name,
     current_article_name,
     standardized_name,
     modal_name,
     SUM(modal_view_instances)         AS modal_view_count,
     SUM(page_view_instances)          AS page_view_count,
     SUM(select_action_instances)      AS select_action_count,
     SUM(spinner_success_instances)    AS spinner_success_count,
     SUM(spinner_failure_instances)    AS spinner_failure_count,
     SUM(toggle_flip_instances)        AS toggle_flips_count,
     denver_date,
     'instances'                       AS unit_type
  FROM asp_page_agg
 WHERE denver_date               >= '${env:START_DATE}'
   AND denver_date               <  '${env:END_DATE}'
GROUP BY
     application_name,
     current_page_name,
     current_article_name,
     standardized_name,
     modal_name,
     denver_date
;

INSERT OVERWRITE TABLE asp_page_agg_counts PARTITION (denver_date, unit_type)
SELECT
     application_name,
     current_page_name,
     current_article_name,
     standardized_name,
     modal_name,
     SUM(modal_view_devices)           AS modal_view_count,
     SUM(page_view_devices)            AS page_view_count,
     SUM(select_action_devices)        AS select_action_count,
     SUM(spinner_success_devices)      AS spinner_success_count,
     SUM(spinner_failure_devices)      AS spinner_failure_count,
     SUM(toggle_flip_devices)          AS toggle_flips_count,
     denver_date,
     'devices'                         AS unit_type
  FROM asp_page_agg
 WHERE denver_date            >= '${env:START_DATE}'
   AND denver_date            <  '${env:END_DATE}'
GROUP BY
     application_name,
     current_page_name,
     current_article_name,
     standardized_name,
     modal_name,
     denver_date
;

INSERT OVERWRITE TABLE asp_page_agg_counts PARTITION (denver_date, unit_type)
SELECT
     application_name,
     current_page_name,
     current_article_name,
     standardized_name,
     modal_name,
     SUM(modal_view_households)        AS modal_view_count,
     SUM(page_view_households)         AS page_view_count,
     SUM(select_action_households)     AS select_action_count,
     SUM(spinner_success_households)   AS spinner_success_count,
     SUM(spinner_failure_households)   AS spinner_failure_count,
     SUM(toggle_flip_households)       AS toggle_flips_count,
     denver_date,
     'households'                      AS unit_type
  FROM asp_page_agg
 WHERE denver_date            >= '${env:START_DATE}'
   AND denver_date            <  '${env:END_DATE}'
GROUP BY
     application_name,
     current_page_name,
     current_article_name,
     standardized_name,
     modal_name,
     denver_date
;
INSERT OVERWRITE TABLE asp_page_agg_counts PARTITION (denver_date, unit_type)
SELECT
     application_name,
     current_page_name,
     current_article_name,
     standardized_name,
     modal_name,
     SUM(modal_view_visits)         AS modal_view_count,
     SUM(page_view_visits)         AS page_view_count,
     SUM(select_action_visits)      AS select_action_count,
     SUM(spinner_success_visits)     AS spinner_success_count,
     SUM(spinner_failure_visits)     AS spinner_failure_count,
     SUM(toggle_flip_visits)        AS toggle_flips_count,
     denver_date,
     'visits'              AS unit_type
  FROM asp_page_agg
 WHERE denver_date            >= '${env:START_DATE}'
   AND denver_date            <  '${env:END_DATE}'
GROUP BY
     application_name,
     current_page_name,
     current_article_name,
     standardized_name,
     modal_name,
     denver_date
;
