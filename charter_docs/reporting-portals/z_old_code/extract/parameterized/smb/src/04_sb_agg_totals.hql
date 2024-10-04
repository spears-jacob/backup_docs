-------------------------------------------------------------------------------

--Combines hive table data and manual adjustment data into tableau feeder table
--Ensures that manual adjustment data "overrides" anything else with same partition
--Adds 'Total Combined' company calculations post-adjustments

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

SELECT '\n\nNow running .... asp_sb_${env:CADENCE}_agg_totals...\n\n';

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin Combined Adjustments + Hive Metrics --

-- Totals -- from aggregated tables)
SELECT '\n\nNow clearing asp_sb_${env:CADENCE}_agg_totals...\n\n';
TRUNCATE TABLE asp_sb_${env:CADENCE}_agg_totals;

SELECT '\n\nNow selecting Total Combined for every metric from ${env:adj} into asp_sb_${env:CADENCE}_agg_totals...\n\n';


INSERT INTO TABLE asp_sb_${env:CADENCE}_agg_totals

SELECT 'Total Combined' as company,
       'sb' as domain,
       metric,
       SUM(value),
       MAX(unit),
       ${env:pf}
FROM ${env:adj}
WHERE ( ("${env:IsReprocess}"=0 AND ${env:pf}="${env:ym}")
          OR
        ("${env:IsReprocess}"=1
          AND SUBSTR(CONCAT(${env:pf},'-01'),0,10) >= "${env:START_DATE}"
          AND SUBSTR(CONCAT(${env:pf},'-01'),0,10) <  "${env:END_DATE}")
      )
GROUP BY metric, ${env:pf}
ORDER BY metric, ${env:pf};


-- End net_products_agg_monthly_tableau TOTALS Insert --
-------------------------------------------------------------------------------

SELECT '\n\nNow Staging non-Total Combined data for every metric from ${env:adj} into asp_sb_${env:CADENCE}_agg_totals...\n\n';

INSERT INTO TABLE asp_sb_${env:CADENCE}_agg_totals

SELECT company,
       'sb' as domain,
       metric,
       value,
       unit,
       ${env:pf}
FROM ${env:adj}
WHERE ( ("${env:IsReprocess}"=0 AND ${env:pf}="${env:ym}")
          OR
        ("${env:IsReprocess}"=1
          AND SUBSTR(CONCAT(${env:pf},'-01'),0,10) >= "${env:START_DATE}"
          AND SUBSTR(CONCAT(${env:pf},'-01'),0,10) <  "${env:END_DATE}")
      )
ORDER BY metric, ${env:pf};
