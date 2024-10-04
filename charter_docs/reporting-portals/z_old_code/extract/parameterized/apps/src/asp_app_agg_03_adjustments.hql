USE ${env:ENVIRONMENT};

SELECT '\n\nNow running asp_app_agg_03_adjustments...\n\n';

-- ADJUSTMENTS (uses insert overwrite to clear partition)
SELECT '\n\nNow inserting adjustments if there are any...\n\n';

INSERT OVERWRITE TABLE asp_app_monthly_adjustments PARTITION(company, year_month, metric)
SELECT unit,
       value,
       change_comment,
       jira_ticket,
       company,
       year_month,
       metric
FROM ${env:TMP_db}.asp_app_monthly_adjustments_raw
WHERE company IS NOT NULL
AND year_month is not null
AND company <> 'company'
AND metric  <> 'Total Subscribers';

-- Manual Files (uses insert overwrite to clear partition)
SELECT '\n\nNow inserting manual Total Subscribers numbers (if included) ...\n\n';

INSERT OVERWRITE TABLE asp_app_monthly_total_subscribers_bi PARTITION(company, year_month)
SELECT metric,
       value,
       company,
       year_month
FROM ${env:TMP_db}.asp_app_monthly_adjustments_raw
WHERE company IS NOT NULL
AND year_month is not null
AND value IS NOT NULL
AND company <> 'company'
AND metric = 'Total Subscribers';
