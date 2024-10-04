-------------------------------------------------------------------------------

--Populates the temp table net_webmail_pv_count with webmail page view counts by company
--Currently populates data from various sources for L-CHTR, L-BHN, and L-TWC

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Webmail Page View Temp Table Calculations --

INSERT INTO TABLE ${env:TMP_db}.net_webmail_pv_count
SELECT
sum(case when UPPER(state__view__current_page__section) ='EMAIL' and message__category ='Page View' THEN 1 ELSE 0 END) AS webmail_page_views_count,
'L-CHTR' AS company,
'${env:YEAR_MONTH}' AS year_month
FROM
net_events
WHERE
partition_date BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
GROUP BY
date_yearmonth(partition_date)
;

SELECT '*****-- END L-CHTR Webmail Page View Temp Table Calculations --*****' -- 14.982 seconds
;

-------------------------------------------------------------------------------

INSERT INTO TABLE ${env:TMP_db}.net_webmail_pv_count
SELECT
bhn_webmail_page_views AS webmail_page_views_count,
'L-BHN' AS company,
'${env:YEAR_MONTH}' AS year_month
FROM
net_webmail_twc_bhn_metrics_monthly
WHERE
partition_year_month = '${env:YEAR_MONTH}'
;

SELECT '*****-- END L-BHN Webmail Page View Temp Table Calculations --*****' -- 0.403 seconds
;

-------------------------------------------------------------------------------

INSERT INTO TABLE ${env:TMP_db}.net_webmail_pv_count
SELECT
twc_webmail_login_page_views AS webmail_page_views_count,
'L-TWC' AS company,
'${env:YEAR_MONTH}' AS year_month
FROM
net_webmail_twc_bhn_metrics_monthly
WHERE
partition_year_month = '${env:YEAR_MONTH}'
;

SELECT '*****-- END L-TWC Webmail Page View Temp Table Calculations --*****' -- 0.405 seconds
;

-- END L-TWC Webmail Page View Temp Table Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- END Webmail Page View Temp Table Calculations --

SELECT '*****-- END Webmail Page View Temp Table Calculations --*****' -- 0.405 seconds
;