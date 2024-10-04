-------------------------------------------------------------------------------
USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Totals -- from aggregated tables)
SELECT '\n\nNow clearing asp_net_${env:CADENCE}_agg_calc...\n\n';
TRUNCATE TABLE asp_net_${env:CADENCE}_agg_calc;
