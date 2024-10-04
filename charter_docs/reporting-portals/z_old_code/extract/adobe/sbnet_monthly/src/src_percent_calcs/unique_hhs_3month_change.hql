-------------------------------------------------------------------------------

-- performs percent calculations for unique_hhs_3month_change metric

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin unique_hhs_3month_change metric calculation insert by company --

INSERT OVERWRITE TABLE ${env:TMP_db}.sbnet_exec_monthly_percents 
PARTITION (company,year_month,metric)

SELECT
NULL AS unit,
NULL AS value_type,
SUM(numerator) / SUM(denominator) AS value,
NULL AS mom_perc_chg,
NULL AS mom_diff,
NULL AS prior_3_mo_perc_chg,
NULL AS prior_3_mo_diff,
NULL AS ytd_avg,
NULL AS prev_months_max_year_month,
NULL AS prev_months_max_val,
NULL AS prev_months_min_year_month,
NULL AS prev_months_min_val,
'Percent_Calculation' AS change_comment,
NULL AS tableau_field,
NULL AS jira_ticket,
company AS company,
'${env:YEAR_MONTH}' AS year_month,
'unique_hhs_3month_change' AS metric
FROM
(SELECT
    tab.company,
    '${env:YEAR_MONTH}' AS year_month,
    CASE WHEN (num1.value IS NOT NULL 
      AND num1.value <> '0' 
      AND den1.value IS NOT NULL 
      AND den1.value <> '0')
        THEN SUM(num1.value)
        ELSE NULL END AS numerator,
    CASE WHEN (den1.value IS NOT NULL 
      AND den1.value <> '0' 
      AND num1.value IS NOT NULL 
      AND num1.value <> '0')
        THEN SUM(den1.value)
        ELSE NULL END AS denominator
    FROM 
    sbnet_exec_monthly_tableau tab
    LEFT JOIN
        (SELECT
        company,
        year_month,
        (value-(LAG(value,3) OVER (PARTITION BY company ORDER BY year_month))) AS value,
        'numerator1' AS numerator1
        FROM 
        sbnet_exec_monthly_tableau
        WHERE
        metric = 'hhs_logged_in'
        AND company <> 'Total Combined'
        ) num1
    ON num1.company = tab.company
        AND num1.year_month = tab.year_month
    LEFT JOIN
        (SELECT
        company,
        year_month,
        (LAG(value,3) OVER (PARTITION BY company ORDER BY year_month)) AS value,
        'denominator1' AS denominator1
        FROM 
        sbnet_exec_monthly_tableau
        WHERE
        metric = 'hhs_logged_in'
        AND company <> 'Total Combined'
        ) den1
    ON den1.company = tab.company
        AND den1.year_month = tab.year_month
    WHERE 
    tab.year_month = '${env:YEAR_MONTH}'
    AND tab.metric = 'unique_hhs_3month_change'
    AND tab.company <> 'Total Combined'
    GROUP BY 
    tab.company,
    num1.value,
    den1.value,
    tab.value
    ) totes
GROUP BY
company,
year_month
;

SELECT '*****-- End unique_hhs_3month_change metric calculation insert by company --*****'
;

-- End unique_hhs_3month_change metric calculation insert by company --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin unique_hhs_3month_change metric calculation insert by company --

INSERT OVERWRITE TABLE ${env:TMP_db}.sbnet_exec_monthly_percents 
PARTITION (company,year_month,metric)

SELECT
NULL AS unit,
NULL AS value_type,
SUM(numerator) / SUM(denominator) AS value,
NULL AS mom_perc_chg,
NULL AS mom_diff,
NULL AS prior_3_mo_perc_chg,
NULL AS prior_3_mo_diff,
NULL AS ytd_avg,
NULL AS prev_months_max_year_month,
NULL AS prev_months_max_val,
NULL AS prev_months_min_year_month,
NULL AS prev_months_min_val,
'Percent_Calculation' AS change_comment,
NULL AS tableau_field,
NULL AS jira_ticket,
'Total Combined' AS company,
'${env:YEAR_MONTH}' AS year_month,
'unique_hhs_3month_change' AS metric
FROM
(SELECT
    tab.company,
    '${env:YEAR_MONTH}' AS year_month,
    CASE WHEN (num1.value IS NOT NULL 
      AND num1.value <> '0' 
      AND den1.value IS NOT NULL 
      AND den1.value <> '0')
        THEN SUM(num1.value)
        ELSE NULL END AS numerator,
    CASE WHEN (den1.value IS NOT NULL 
      AND den1.value <> '0' 
      AND num1.value IS NOT NULL 
      AND num1.value <> '0')
        THEN SUM(den1.value)
        ELSE NULL END AS denominator
    FROM 
    sbnet_exec_monthly_tableau tab
    LEFT JOIN
        (SELECT
        company,
        year_month,
        (value-(LAG(value,1) OVER (PARTITION BY company ORDER BY year_month))) AS value,
        'numerator1' AS numerator1
        FROM 
        sbnet_exec_monthly_tableau
        WHERE
        metric = 'hhs_logged_in'
        AND company <> 'Total Combined'
        ) num1
    ON num1.company = tab.company
        AND num1.year_month = tab.year_month
    LEFT JOIN
        (SELECT
        company,
        year_month,
        (LAG(value,1) OVER (PARTITION BY company ORDER BY year_month)) AS value,
        'denominator1' AS denominator1
        FROM 
        sbnet_exec_monthly_tableau
        WHERE
        metric = 'hhs_logged_in'
        AND company <> 'Total Combined'
        ) den1
    ON den1.company = tab.company
        AND den1.year_month = tab.year_month
    WHERE 
    tab.year_month = '${env:YEAR_MONTH}'
    AND tab.metric = 'unique_hhs_3month_change'
    AND tab.company <> 'Total Combined'
    GROUP BY 
    tab.company,
    num1.value,
    den1.value,
    tab.value
    ) totes
GROUP BY
'Total Combined',
year_month
;

SELECT '*****-- End unique_hhs_3month_change metric calculation insert by Total Combined --*****'
;

-- End unique_hhs_3month_change metric calculation insert by Total Combined --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------