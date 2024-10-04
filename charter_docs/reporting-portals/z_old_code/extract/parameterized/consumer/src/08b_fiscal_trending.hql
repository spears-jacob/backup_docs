USE ${env:ENVIRONMENT};

-----------------------------------------------------
--STEP 1
--Create numerators and denominators for monthly trending
-----------------------------------------------------

INSERT OVERWRITE TABLE asp_net_fiscal_monthly_trend_pre PARTITION (company,year_month,metric)
SELECT
    mon.unit,
    mon.value,
    mon.domain,
    COALESCE(names.tableau_display,mon.metric) AS tableau_display,
--Month over Month
    CASE WHEN (math.num1m IS NOT NULL
      AND math.num1m <> '0'
      AND math.den1m IS NOT NULL
      AND math.den1m <> '0')
        THEN SUM(math.num1m)
        ELSE NULL END AS numerator1,
    CASE WHEN (math.den1m IS NOT NULL
      AND math.den1m <> '0'
      AND math.num1m IS NOT NULL
      AND math.num1m <> '0')
        THEN SUM(math.den1m)
        ELSE NULL END AS denominator1,
--3 Months Change
    CASE WHEN (math.num3m IS NOT NULL
      AND math.num3m <> '0'
      AND math.den3m IS NOT NULL
      AND math.den3m <> '0')
        THEN SUM(math.num3m)
        ELSE NULL END AS numerator3,
    CASE WHEN (math.den3m IS NOT NULL
      AND math.den3m <> '0'
      AND math.num3m IS NOT NULL
      AND math.num3m <> '0')
        THEN SUM(math.den3m)
        ELSE NULL END AS denominator3,
--Year over Year
    CASE WHEN (math.num12m IS NOT NULL
      AND math.num12m <> '0'
      AND math.den12m IS NOT NULL
      AND math.den12m <> '0')
        THEN SUM(math.num12m)
        ELSE NULL END AS numerator12,
    CASE WHEN (math.den12m IS NOT NULL
      AND math.den12m <> '0'
      AND math.num12m IS NOT NULL
      AND math.num12m <> '0')
        THEN SUM(math.den12m)
        ELSE NULL END AS denominator12,
    mon.company,
    mon.year_fiscal_month_denver AS year_month,
    mon.metric
    FROM
    agg_fiscal_monthly mon
    LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm on mon.year_fiscal_month_denver = fm.fiscal_month
    LEFT JOIN ${env:TMP_db}.net_products_monthly_tableau_metric_lkp names
    ON mon.metric = names.hive_metric
--Month over Month
    LEFT JOIN
        asp_net_fiscal_monthly_trend_lags math
    ON math.company = mon.company
        AND math.year_fiscal_month_denver = mon.year_fiscal_month_denver
        AND math.metric = mon.metric
    WHERE
        --mon.company <> 'Total Combined'
        --AND
        mon.platform = 'asp'
        AND mon.domain = 'resi'
        AND (fm.partition_date >= ("${env:START_DATE}") AND fm.partition_date < ("${env:END_DATE}"))
    GROUP BY
    mon.company,
    mon.year_fiscal_month_denver,
    mon.metric,
    mon.unit,
    mon.domain,
    mon.value,
    COALESCE(names.tableau_display,mon.metric),
    math.num1m,
    math.den1m,
    math.num3m,
    math.den3m,
    math.num12m,
    math.den12m
;

SELECT '

*****-- End STEP 1: Create numerators and denominators for monthly trending --*****

';

-----------------------------------------------------
--STEP 2
--Calculate percentages for monthly trending
-----------------------------------------------------

INSERT OVERWRITE TABLE asp_net_fiscal_monthly_trend_perc PARTITION (company,year_month,metric)
SELECT
unit,
value,
SUM(numerator1) / SUM(denominator1) AS mom_perc_chg,
SUM(numerator3) / SUM(denominator3) AS prior_3_mo_perc_chg,
SUM(numerator12) / SUM(denominator12) AS yoy_perc_chg,
domain,
tableau_display,
company,
year_month,
metric
FROM
asp_net_fiscal_monthly_trend_pre pre
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm on pre.year_month = fm.fiscal_month
WHERE (fm.partition_date >= ("${env:START_DATE}") AND fm.partition_date < ("${env:END_DATE}"))
GROUP BY
company,
year_month,
metric,
unit,
value,
domain,
tableau_display
;

SELECT '

*****-- End STEP 1: Calculate percentages for monthly trending --*****

';

-----------------------------------------------------
--- ***** END MONTHLY TRENDING CALCULATIONS ***** ---
-----------------------------------------------------

