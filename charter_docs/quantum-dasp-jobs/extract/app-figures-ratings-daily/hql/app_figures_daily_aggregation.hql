USE ${env:DASP_db};

SET AppFiguresLag=14;
set hive.exec.dynamic.partition.mode=nonstrict;
SELECT "\n\nThe AppFiguresLag variable is used to set how long to lag the app figures counts and reviews,\n\n now set to ${hiveconf:AppFiguresLag} days.\n\n";

SELECT '\n\nNow running 02_app_figures_daily_aggregation...\n\n';
SELECT CONCAT("Starting Date is ", cast(DATE_SUB("${hiveconf:RUN_DATE}",${hiveconf:AppFiguresLag}) as string),"\n  Ending Date is ${hiveconf:RUN_DATE}\n\n");

-- (uses insert overwrite to clear partition)
SELECT '\n\nNow selecting App Downloads by App Store...\n\n';

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_app_daily_app_figures PARTITION(company, date_denver)
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
partition_date_denver as date_denver
FROM app_figures_downloads
where (  partition_date_denver >= DATE_SUB("${hiveconf:RUN_DATE}",${hiveconf:AppFiguresLag})
    AND  partition_date_denver < "${hiveconf:RUN_DATE}" )
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
and (daily_downloads IS NOT NULL)
group by product_name, store, partition_date_denver;


SELECT '\n\nNow selecting App Downloads across all stores...\n\n';

INSERT INTO TABLE ${env:DASP_db}.asp_app_daily_app_figures PARTITION(company, date_denver)
SELECT 'App Downloads' AS metric,
SUM(daily_downloads) as value,
CASE WHEN product_name = 'My BHN' THEN 'BHN'
     WHEN product_name = 'My TWC®' THEN 'TWCC'
     WHEN product_name = 'My Spectrum' THEN 'CHTR'
     ELSE 'UNDEFINED'
END as company,
partition_date_denver as date_denver
FROM app_figures_downloads
where (partition_date_denver >= DATE_SUB("${hiveconf:RUN_DATE}",${hiveconf:AppFiguresLag})
    AND (partition_date_denver < "${hiveconf:RUN_DATE}"))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
AND daily_downloads IS NOT NULL
group by product_name, partition_date_denver;


SELECT '\n\nNow selecting App Updates by App Store...\n\n';

INSERT INTO TABLE ${env:DASP_db}.asp_app_daily_app_figures PARTITION(company, date_denver)
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
partition_date_denver as date_denver
FROM app_figures_downloads
where (  partition_date_denver >= DATE_SUB("${hiveconf:RUN_DATE}",${hiveconf:AppFiguresLag})
    AND  partition_date_denver < "${hiveconf:RUN_DATE}" )
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
and (daily_updates IS NOT NULL)
group by product_name, store, partition_date_denver;


SELECT '\n\nNow selecting App Updates across all stores...\n\n';

INSERT INTO TABLE ${env:DASP_db}.asp_app_daily_app_figures PARTITION(company, date_denver)
SELECT 'App Updates' AS metric,
SUM(daily_updates) as value,
CASE WHEN product_name = 'My BHN' THEN 'BHN'
     WHEN product_name = 'My TWC®' THEN 'TWCC'
     WHEN product_name = 'My Spectrum' THEN 'CHTR'
     ELSE 'UNDEFINED'
END as company,
partition_date_denver as date_denver
FROM app_figures_downloads
where (partition_date_denver >= DATE_SUB("${hiveconf:RUN_DATE}",${hiveconf:AppFiguresLag})
    AND (partition_date_denver < "${hiveconf:RUN_DATE}"))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
AND daily_updates IS NOT NULL
group by product_name, partition_date_denver;


SELECT '\n\nNow selecting App reviews across all stores...\n\n';

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_app_daily_app_figures_reviews PARTITION(date_denver)
SELECT
CASE WHEN product_name = 'My BHN' THEN 'BHN'
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
partition_date_denver as date_denver
FROM app_figures_sentiment
where (partition_date_denver >= DATE_SUB("${hiveconf:RUN_DATE}",${hiveconf:AppFiguresLag})
    AND (partition_date_denver < "${hiveconf:RUN_DATE}"))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
ORDER BY date_denver, company, platform
;
