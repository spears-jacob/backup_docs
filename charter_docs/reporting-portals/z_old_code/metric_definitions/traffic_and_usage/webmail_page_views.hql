-- QUERY DEFINITIONS FOR webmail_page_views
-- LAST UPDATED 2017-08-09 BY Zach Jesberger

USE {env:ENVIRONMENT};

-- NOTE - All queries set to calculate for most recent day (Denver Time)

-- ***** RESIDENTIAL PORTAL QUERIES *****
-- L-CHTR Query
SELECT
'RES' as portal_platform,
'L-CHTR' as legacy_footprint,
sum(if(UPPER(state__view__current_page__section) ='EMAIL' and message__category ='Page View',1,0)) as webmail_page_views
FROM net_events
WHERE partition_date = DATE_SUB(current_date,1)
;
-- L-TWC Query
SELECT
'RES' as portal_platform,
'L-TWC' as legacy_footprint,
twc_webmail_login_page_views as webmail_page_views
FROM net_webmail_twc_bhn_metrics_monthly
WHERE partition_year_month = '${env:YEAR_MONTH}'
;
-- L-BHN Query
-- Already Aggregated Monthly
SELECT
'RES' as portal_platform,
'L-BHN' as legacy_footprint,
bhn_webmail_page_views as webmail_page_views
FROM net_webmail_twc_bhn_metrics_monthly
WHERE partition_year_month = '${env:YEAR_MONTH}'
;


-- ***** SMB PORTAL QUERIES *****
-- L-CHTR Query
-- N/A - No Webmail Feature for SMB

-- L-TWC Query
-- N/A - No Webmail Feature for SMB

-- L-BHN Query
-- N/A - No Webmail Feature for SMB


-- ***** APP QUERIES *****
-- L-CHTR Query


-- L-TWC Query


-- L-BHN Query
