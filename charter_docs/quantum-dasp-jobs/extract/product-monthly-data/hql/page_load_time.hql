USE ${env:DASP_db};

set hive.exec.max.dynamic.partitions = 8000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled = false;
set orc.force.positional.evolution=true;
SET hive.merge.tezfiles=true;
set hive.merge.mapredfiles=true;

SELECT "\n\nFor 1: Page_Load_Time\n\n";

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_product_monthly_time partition (label_date_denver)
SELECT domain,
       SUM(1)                                                           as num_total,
       SUM(IF(avg_load_visit < 2, 1, 0))                                AS num_less_than2,
       SUM(IF(avg_load_visit >= 2 and avg_load_visit < 4, 1, 0))        AS num_between2_and4,
       SUM(IF(avg_load_visit >= 4 and avg_load_visit < 6, 1, 0))        AS num_between4_and6,
       SUM(IF(avg_load_visit >= 6, 1, 0))                               AS num_larger_than6,
       SUM(IF(avg_load_visit < 2, 1, 0))/SUm(1) AS pct_less_than2,
       SUM(IF(avg_load_visit >= 2 and avg_load_visit < 4, 1, 0))/SUm(1) AS pct_between2_and4,
       SUM(IF(avg_load_visit >= 4 and avg_load_visit < 6, 1, 0))/SUm(1) AS pct_between4_and6,
       SUM(IF(avg_load_visit >= 6, 1, 0))/SUm(1) AS pct_larger_than6,
       current_date as run_date,
       '${hiveconf:label_date_denver}' as label_date_denver
  FROM
      (SELECT domain,
              visit_id,
              AVG(pg_load_sec_tenths) avg_load_visit
         FROM (SELECT
                    CASE WHEN visit__application_details__application_name = 'SpecNet' THEN 'Spectrum.net'
                         WHEN visit__application_details__application_name = 'SMB'     THEN 'SpectrumBusiness.net'
                    END AS domain,
                    visit__visit_id as visit_id,
                    state__view__current_page__page_name AS page_name,
                    ROUND((state__view__current_page__performance_timing__dom_complete -
                           state__view__current_page__performance_timing__navigation_start)/1000,1) AS  pg_load_sec_tenths
              FROM ${env:ENVIRONMENT}.core_quantum_events_sspp
             WHERE (partition_date_utc >= '${hiveconf:START_DATE}' AND partition_date_utc < '${hiveconf:END_DATE}')
               and visit__application_details__application_name IN ('SpecNet', 'SMB')
               AND message__name = 'pageViewPerformance'
               AND (state__view__current_page__performance_timing__dom_complete -
                    state__view__current_page__performance_timing__navigation_start) > 0
             ) dictionary
      GROUP BY domain,
               visit_id) visit_avg
GROUP BY domain
;
