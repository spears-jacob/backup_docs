USE ${env:ENVIRONMENT};

set hive.exec.max.dynamic.partitions = 8000;

set hive.vectorized.execution.enabled = false;

SELECT "\n\nFor 1: Page_Load_Time\n\n";

INSERT OVERWRITE TABLE asp_product_monthly_time partition(label_date_denver)
Select domain,
       SUm(1) as num_total,
       SUM(IF(avg_load_visit < 2, 1, 0)) AS num_less_than2,
       SUM(IF(avg_load_visit >= 2 and avg_load_visit < 4, 1, 0)) AS num_between2_and4,
       SUM(IF(avg_load_visit >= 4 and avg_load_visit < 6, 1, 0)) AS num_between4_and6,
       SUM(IF(avg_load_visit >= 6, 1, 0)) AS num_larger_than6,
       SUM(IF(avg_load_visit < 2, 1, 0))/SUm(1) AS pct_less_than2,
       SUM(IF(avg_load_visit >= 2 and avg_load_visit < 4, 1, 0))/SUm(1) AS pct_between2_and4,
       SUM(IF(avg_load_visit >= 4 and avg_load_visit < 6, 1, 0))/SUm(1) AS pct_between4_and6,
       SUM(IF(avg_load_visit >= 6, 1, 0))/SUm(1) AS pct_larger_than6,
       current_date as run_date,
       '${env:label_date_denver}' as label_date_denver
  FROM
      (SELECT domain,
              visit_id,
              AVG(pg_load_sec_tenths) avg_load_visit
         FROM (SELECT
                    CASE WHEN lower(visit__application_details__application_name) = 'specnet' THEN 'Spectrum.net'
                         WHEN lower(visit__application_details__application_name) = 'smb'     THEN 'SpectrumBusiness.net'
                    END AS domain,
                    visit__visit_id as visit_id,
                    state__view__current_page__page_name AS page_name,
                    ROUND((state__view__current_page__performance_timing__dom_complete -
                           state__view__current_page__performance_timing__navigation_start)/1000,1) AS  pg_load_sec_tenths
              FROM asp_v_venona_events_portals
             WHERE ( partition_date_utc  >= '${env:START_DATE}' AND partition_date_utc < '${env:END_DATE}')
               AND message__name = 'pageViewPerformance'
               and lower(visit__application_details__application_name) in ('specnet', 'smb')
               AND (state__view__current_page__performance_timing__dom_complete -
                    state__view__current_page__performance_timing__navigation_start) > 0
             ) dictionary
      GROUP BY domain,
               visit_id) visit_avg
group by domain
;
