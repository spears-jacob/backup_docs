-------------------------------------------------------------------------------

--Populates the temp table net_webmail_pv_count with webmail page view counts by company
--Currently populates data from various sources for L-CHTR, L-BHN, and L-TWC

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Webmail Page View Temp Table Calculations --

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM (  SELECT COUNT(1) AS value,
               'instances' as unit,
               'resi' AS domain,
               'L-CHTR' AS company,
               ${env:pf},
               'webmail_page_views_count' as metric
        FROM    (SELECt  ${env:ap} as ${env:pf},
                         message__category,
                         state__view__current_page__section
                 FROM asp_v_net_events ne
                 LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm on ne.partition_date  = fm.partition_date
                 WHERE (ne.partition_date >= ("${env:START_DATE}") AND ne.partition_date < ("${env:END_DATE}"))
                 AND UPPER(state__view__current_page__section) ='EMAIL'
                 AND message__category ='Page View'
               ) dictionary
        GROUP BY ${env:pf}
      ) q
;

SELECT '*****-- END L-CHTR Webmail Page View Temp Table Calculations --*****' -- 14.982 seconds
;

-------------------------------------------------------------------------------
INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  ( SELECT  MAX(bhn_webmail_page_views) AS value,
                'visits' as unit,
                'resi' as domain,
                "webmail_page_views_count" as metric,
                'L-BHN' AS company,
                partition_year_month as ${env:pf}
        FROM asp_net_webmail_twc_bhn_monthly
        WHERE partition_year_month = "${env:ym}"
        AND "${env:CADENCE}" <> 'daily'
        GROUP BY partition_year_month
      ) q
;

SELECT '*****-- END L-BHN Webmail Page View Temp Table Calculations --*****' -- 0.403 seconds
;

-------------------------------------------------------------------------------

INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

SELECT value, unit, domain, company, ${env:pf}, metric
FROM  ( SELECT  MAX(twc_webmail_page_views) AS value,
                'visits' as unit,
                'resi' as domain,
                "webmail_page_views_count" as metric,
                'L-TWC' AS company,
                partition_year_month as ${env:pf}
        FROM asp_net_webmail_twc_bhn_monthly
        WHERE partition_year_month = "${env:ym}"
        AND "${env:CADENCE}" <> 'daily'
        GROUP BY partition_year_month
      ) q
;



SELECT '*****-- END L-TWC Webmail Page View Temp Table Calculations --*****' -- 0.405 seconds
;

-- END L-TWC Webmail Page View Temp Table Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- END Webmail Page View Temp Table Calculations --

SELECT '*****-- END Webmail Page View Temp Table Calculations --*****' -- 0.405 seconds
;
