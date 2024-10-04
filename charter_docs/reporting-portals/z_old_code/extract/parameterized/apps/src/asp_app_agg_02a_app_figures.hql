USE ${env:ENVIRONMENT};

SET AppFiguresLag=14;
SELECT "\n\nThe AppFiguresLag variable is used to set how long to lag the app figures counts and reviews,\n\n now set to ${hiveconf:AppFiguresLag} days.\n\n";

SELECT '\n\nNow running asp_app_agg_02a_app_figures...\n\n';
SELECT CONCAT("Starting Date is ", cast(DATE_SUB("${env:START_DATE}",${hiveconf:AppFiguresLag}) as string),"\n  Ending Date is ${env:END_DATE}\n\n");

-- (uses insert overwrite to clear partition)
SELECT '\n\nNow selecting App Downloads by App Store...\n\n';

INSERT OVERWRITE TABLE asp_app_${env:CADENCE}_app_figures PARTITION(company, ${env:ymd})
SELECT  CASE WHEN store = 'google_play' THEN 'App Downloads Android'
             WHEN store = 'apple' THEN 'App Downloads iOS'
             ELSE 'UNDEFINED'
        END AS metric,
SUM(daily_downloads) as value,
CASE WHEN product_name = 'My BHN' THEN 'BHN'
     WHEN product_name = 'My TWC®' THEN 'TWCC'
     WHEN product_name = 'My Spectrum' THEN 'CHTR'
     ELSE 'UNDEFINED'
END as company,
${env:afp} as ${env:ymd}
FROM asp_v_app_figures_downloads
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on partition_date_denver = partition_date
where (  partition_date_denver >= DATE_SUB("${env:START_DATE}",${hiveconf:AppFiguresLag})
    AND  partition_date_denver < "${env:END_DATE}" )
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
and (daily_downloads IS NOT NULL)
group by product_name, store, ${env:afp};


SELECT '\n\nNow selecting App Downloads across all stores...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_app_figures PARTITION(company, ${env:ymd})
SELECT 'App Downloads' AS metric,
SUM(daily_downloads) as value,
CASE WHEN product_name = 'My BHN' THEN 'BHN'
     WHEN product_name = 'My TWC®' THEN 'TWCC'
     WHEN product_name = 'My Spectrum' THEN 'CHTR'
     ELSE 'UNDEFINED'
END as company,
${env:afp} as ${env:ymd}
FROM asp_v_app_figures_downloads
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on partition_date_denver = partition_date
where (partition_date_denver >= DATE_SUB("${env:START_DATE}",${hiveconf:AppFiguresLag})
    AND (partition_date_denver < "${env:END_DATE}"))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
AND daily_downloads IS NOT NULL
group by product_name, ${env:afp};


SELECT '\n\nNow selecting App Updates by App Store...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_app_figures PARTITION(company, ${env:ymd})
SELECT  CASE WHEN store = 'google_play' THEN 'App Updates Android'
             WHEN store = 'apple' THEN 'App Updates iOS'
             ELSE 'UNDEFINED'
        END AS metric,
SUM(daily_updates) as value,
CASE WHEN product_name = 'My BHN' THEN 'BHN'
     WHEN product_name = 'My TWC®' THEN 'TWCC'
     WHEN product_name = 'My Spectrum' THEN 'CHTR'
     ELSE 'UNDEFINED'
END as company,
${env:afp} as ${env:ymd}
FROM asp_v_app_figures_downloads
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on partition_date_denver = partition_date
where (  partition_date_denver >= DATE_SUB("${env:START_DATE}",${hiveconf:AppFiguresLag})
    AND  partition_date_denver < "${env:END_DATE}" )
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
and (daily_updates IS NOT NULL)
group by product_name, store, ${env:afp};


SELECT '\n\nNow selecting App Updates across all stores...\n\n';

INSERT INTO TABLE asp_app_${env:CADENCE}_app_figures PARTITION(company, ${env:ymd})
SELECT 'App Updates' AS metric,
SUM(daily_updates) as value,
CASE WHEN product_name = 'My BHN' THEN 'BHN'
     WHEN product_name = 'My TWC®' THEN 'TWCC'
     WHEN product_name = 'My Spectrum' THEN 'CHTR'
     ELSE 'UNDEFINED'
END as company,
${env:afp} as ${env:ymd}
FROM asp_v_app_figures_downloads
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on partition_date_denver = partition_date
where (partition_date_denver >= DATE_SUB("${env:START_DATE}",${hiveconf:AppFiguresLag})
    AND (partition_date_denver < "${env:END_DATE}"))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
AND daily_updates IS NOT NULL
group by product_name, ${env:afp};


SELECT '\n\nNow selecting App reviews across all stores...\n\n';

INSERT OVERWRITE TABLE asp_app_${env:CADENCE}_app_figures_reviews PARTITION(${env:ymd})
SELECT  CASE WHEN product_name = 'My BHN' THEN 'BHN'
             WHEN product_name = 'My TWC?' THEN 'TWCC'
             WHEN product_name = 'My Spectrum' THEN 'CHTR'
             ELSE 'UNDEFINED'
        END as company,
CASE WHEN store = 'google_play' THEN 'Android'
             WHEN store = 'apple' THEN 'iOS'
             ELSE 'UNDEFINED'
        END AS platform,
stars,
review,
${env:afp} as ${env:ymd}
FROM asp_v_app_figures_sentiment
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} on partition_date_denver = partition_date
where (partition_date_denver >= DATE_SUB("${env:START_DATE}",${hiveconf:AppFiguresLag})
    AND (partition_date_denver < "${env:END_DATE}"))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
ORDER BY ${env:ymd}, company, platform
;
