USE ${env:ENVIRONMENT};

SELECT '\n\nNow running asp_app_agg_truncate...\n\n';

TRUNCATE TABLE asp_app_${env:CADENCE}_agg_calc;
