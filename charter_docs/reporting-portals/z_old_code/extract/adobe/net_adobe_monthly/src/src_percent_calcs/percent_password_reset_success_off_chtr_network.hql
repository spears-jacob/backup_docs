SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-------------------------------------------------------------------------------

-- performs percent calculations for percent_password_reset_success_off_chtr_network metric

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin percent_password_reset_success_off_chtr_network metric calculation insert by company --

INSERT INTO ${env:TMP_db}.net_products_agg_monthly_percents 
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
'percent_password_reset_success_off_chtr_network' AS metric
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
    net_products_agg_monthly_tableau tab
    LEFT JOIN
        (SELECT
        company,
        year_month,
        SUM(value) AS value,
        'numerator1' AS numerator1
        FROM 
        net_products_agg_monthly_tableau
        WHERE
        metric = 'successful_reset_password_count_off_net'
        AND year_month = '${env:YEAR_MONTH}'
        AND company <> 'Total Combined'
        GROUP BY
        year_month,
        company
        ) num1
    ON num1.company = tab.company
        AND num1.year_month = tab.year_month
    LEFT JOIN
        (SELECT
        company,
        year_month,
        SUM(value) AS value,
        'denominator1' AS denominator1
        FROM 
        net_products_agg_monthly_tableau
        WHERE
        metric = 'attempts_reset_password_count_off_net'
        AND year_month = '${env:YEAR_MONTH}'
        AND company <> 'Total Combined'
        GROUP BY
        year_month,
        company
        ) den1
    ON den1.company = tab.company
        AND den1.year_month = tab.year_month
    WHERE 
    tab.year_month = '${env:YEAR_MONTH}'
    AND tab.metric = 'percent_password_reset_success_off_chtr_network'
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

SELECT '*****-- End percent_password_reset_success_off_chtr_network metric calculation insert by company --*****'
;

-- End percent_password_reset_success_off_chtr_network metric calculation insert by company --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin percent_password_reset_success_off_chtr_network metric calculation insert by company --

INSERT INTO ${env:TMP_db}.net_products_agg_monthly_percents 
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
'percent_password_reset_success_off_chtr_network' AS metric
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
    net_products_agg_monthly_tableau tab
    LEFT JOIN
        (SELECT
        company,
        year_month,
        SUM(value) AS value,
        'numerator1' AS numerator1
        FROM 
        net_products_agg_monthly_tableau
        WHERE
        metric = 'successful_reset_password_count_off_net'
        AND year_month = '${env:YEAR_MONTH}'
        AND company <> 'Total Combined'
        GROUP BY
        year_month,
        company
        ) num1
    ON num1.company = tab.company
        AND num1.year_month = tab.year_month
    LEFT JOIN
        (SELECT
        company,
        year_month,
        SUM(value) AS value,
        'denominator1' AS denominator1
        FROM 
        net_products_agg_monthly_tableau
        WHERE
        metric = 'attempts_reset_password_count_off_net'
        AND year_month = '${env:YEAR_MONTH}'
        AND company <> 'Total Combined'
        GROUP BY
        year_month,
        company
        ) den1
    ON den1.company = tab.company
        AND den1.year_month = tab.year_month
    WHERE 
    tab.year_month = '${env:YEAR_MONTH}'
    AND tab.metric = 'percent_password_reset_success_off_chtr_network'
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

SELECT '*****-- End percent_password_reset_success_off_chtr_network metric calculation insert by Total Combined --*****'
;

-- End percent_password_reset_success_off_chtr_network metric calculation insert by Total Combined --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------