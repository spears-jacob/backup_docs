-------------------------------------------------------------------------------

--Populates the temp table with aggregated data for L-BHN

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-- Drop and rebuild net_bill_pay_analytics_monthly temp table --
DROP TABLE IF EXISTS ${env:TMP_db}.net_bhn_page_views_${env:CADENCE};

-- For troubleshooting queries, remove 'TEMPORARY'
CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.net_bhn_page_views_${env:CADENCE}
AS
  SELECT
    SUM(IF(ev.message__category = 'Page View',1,0)) AS total_page_views_count,
    SUM(IF(ev.message__category = 'Page View' and LOWER(ev.site_section_prop6) IN('my services active','my services'),1,0)) AS my_account_page_views_count,
    SUM(IF(ev.message__category = 'Page View' and LOWER(ev.state__view__current_page__section) = 'support active',1,0)) AS support_page_views_count,
    'L-BHN' AS company,
    SIZE(COLLECT_SET(IF(visit__visit_id IS NOT NULL, visit__visit_id, NULL))) AS visits,
    ${env:pf}
  FROM (SELECT message__category,
               state__view__current_page__section,
               visit__settings["post_prop6"] AS site_section_prop6,
               visit__visit_id,
               ${env:ap} as ${env:pf}
        FROM asp_v_bhn_residential_events
        LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on ${env:ape}  = partition_date
        WHERE (partition_date_hour_utc >= ("${env:START_DATE_TZ}") AND partition_date_hour_utc < ("${env:END_DATE_TZ}"))
        --AND message__category = 'Page View'
      ) ev
  GROUP BY ${env:pf}
;


SELECT '*****-- End L-BHN net_products_agg_monthly Temp Table Insert --*****' -- 176.414 seconds
;

-- End L-BHN net_products_agg_monthly Temp Table Insert --
-------------------------------------------------------------------------------

----- Insert into agg table -------
INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  (SELECT  'total_page_views_count' as metric,
               total_page_views_count AS value,
               'page views' as unit,
               'resi' as domain,
               'L-BHN' AS company,
               ${env:pf}
      FROM ${env:TMP_db}.net_bhn_page_views_${env:CADENCE}
      UNION
      SELECT  'my_account_page_views_count' as metric,
               my_account_page_views_count AS value,
               'page views' as unit,
               'resi' as domain,
               'L-BHN',
               ${env:pf}
      FROM ${env:TMP_db}.net_bhn_page_views_${env:CADENCE}
      UNION
      SELECT  'support_page_views_count' as metric,
               support_page_views_count AS value,
               'page views' as unit,
               'resi' as domain,
               'L-BHN',
               ${env:pf}
      FROM ${env:TMP_db}.net_bhn_page_views_${env:CADENCE}
      UNION
      SELECT   'visits' as metric,
               visits AS value,
               'visits' as unit,
               'resi' as domain,
               'L-BHN',
               ${env:pf}
      FROM ${env:TMP_db}.net_bhn_page_views_${env:CADENCE} ) q
;
