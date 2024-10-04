USE ${env:ENVIRONMENT};

--------------------------------------------------------------------------------
------------------------- *** Construct variables *** --------------------------
--------------------------------------------------------------------------------

SET date_lag = 15;



SELECT CONCAT('\n\nEND Construct variables

date_lag = ',${hiveconf:date_lag},'\n\n')
;

--------------------------------------------------------------------------------
---------------- *** Split running total into daily counts *** -----------------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE asp_app_figures_star_ratings_daily PARTITION (product_id,platform,version,partition_date_denver)
select  one_star AS one_star_to_date,
        two_star AS two_star_to_date,
        thr_star AS thr_star_to_date,
        fou_star AS fou_star_to_date,
        fiv_star AS fiv_star_to_date,
        one_star - (LAG(one_star,1) OVER (PARTITION BY store,version ORDER BY store,version,partition_date_denver)) AS one_star_daily,
        two_star - (LAG(two_star,1) OVER (PARTITION BY store,version ORDER BY store,version,partition_date_denver)) AS two_star_daily,
        thr_star - (LAG(thr_star,1) OVER (PARTITION BY store,version ORDER BY store,version,partition_date_denver)) AS thr_star_daily,
        fou_star - (LAG(fou_star,1) OVER (PARTITION BY store,version ORDER BY store,version,partition_date_denver)) AS fou_star_daily,
        fiv_star - (LAG(fiv_star,1) OVER (PARTITION BY store,version ORDER BY store,version,partition_date_denver)) AS fiv_star_daily,
        product_name,
        product_id,
        CASE
          WHEN store = 'apple' THEN 'iOS'
          WHEN store = 'google_play' THEN 'Android'
          ELSE NULL END AS platform,
        version,
        partition_date_denver
FROM asp_v_app_figures_downloads
WHERE (partition_date_denver BETWEEN DATE_SUB('${env:RUN_DATE}',${hiveconf:date_lag})
                             AND '${env:RUN_DATE}')
AND product_id in ('40423315838','40425298890')
ORDER BY  platform,
          version,
          partition_date_denver
;

SELECT '\n\nEND Split running total into daily counts\n\n';

SELECT '\n\nBEGIN App Figure Aggregations\n\n';

SET AppFiguresLag=14;

SELECT "\n\nThe AppFiguresLag variable is used to set how long to lag the app figures counts and reviews,\n\n now set to ${hiveconf:AppFiguresLag} days.\n\n";

SELECT '\n\nNow running 02_app_figures_daily_aggregation...\n\n';

SELECT CONCAT("Starting Date is ", cast(DATE_SUB("${env:RUN_DATE}",${hiveconf:AppFiguresLag}) as string),"\n  Ending Date is ${env:RUN_DATE}\n\n");

-- (uses insert overwrite to clear partition)
SELECT '\n\nNow selecting App Downloads by App Store...\n\n';

INSERT OVERWRITE TABLE asp_app_daily_app_figures PARTITION(company, date_denver)
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
FROM asp_v_app_figures_downloads
where (  partition_date_denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:AppFiguresLag})
    AND  partition_date_denver < "${env:RUN_DATE}" )
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
and (daily_downloads IS NOT NULL)
group by product_name, store, partition_date_denver;


SELECT '\n\nNow selecting App Downloads across all stores...\n\n';

INSERT INTO TABLE asp_app_daily_app_figures PARTITION(company, date_denver)
SELECT 'App Downloads' AS metric,
SUM(daily_downloads) as value,
CASE WHEN product_name = 'My BHN' THEN 'BHN'
     WHEN product_name = 'My TWC®' THEN 'TWCC'
     WHEN product_name = 'My Spectrum' THEN 'CHTR'
     ELSE 'UNDEFINED'
END as company,
partition_date_denver as date_denver
FROM asp_v_app_figures_downloads
where (partition_date_denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:AppFiguresLag})
    AND (partition_date_denver < "${env:RUN_DATE}"))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
AND daily_downloads IS NOT NULL
group by product_name, partition_date_denver;


SELECT '\n\nNow selecting App Updates by App Store...\n\n';

INSERT INTO TABLE asp_app_daily_app_figures PARTITION(company, date_denver)
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
FROM asp_v_app_figures_downloads
where (  partition_date_denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:AppFiguresLag})
    AND  partition_date_denver < "${env:RUN_DATE}" )
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
and (daily_updates IS NOT NULL)
group by product_name, store, partition_date_denver;


SELECT '\n\nNow selecting App Updates across all stores...\n\n';

INSERT INTO TABLE asp_app_daily_app_figures PARTITION(company, date_denver)
SELECT 'App Updates' AS metric,
SUM(daily_updates) as value,
CASE WHEN product_name = 'My BHN' THEN 'BHN'
     WHEN product_name = 'My TWC®' THEN 'TWCC'
     WHEN product_name = 'My Spectrum' THEN 'CHTR'
     ELSE 'UNDEFINED'
END as company,
partition_date_denver as date_denver
FROM asp_v_app_figures_downloads
where (partition_date_denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:AppFiguresLag})
    AND (partition_date_denver < "${env:RUN_DATE}"))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
AND daily_updates IS NOT NULL
group by product_name, partition_date_denver;


SELECT '\n\nNow selecting App reviews across all stores...\n\n';

INSERT OVERWRITE TABLE asp_app_daily_app_figures_reviews PARTITION(date_denver)
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
FROM asp_v_app_figures_sentiment
where (partition_date_denver >= DATE_SUB("${env:RUN_DATE}",${hiveconf:AppFiguresLag})
    AND (partition_date_denver < "${env:RUN_DATE}"))
AND product_id IN (40423315838,40425298890,11340139,15570967,'40423315838.0','40425298890.0')
ORDER BY date_denver, company, platform
;

SELECT '\n\n ***** END App Figure Aggregations ***** \n\n';

--------------------------------------------------------------------------------
--------------------------------- *** END *** ----------------------------------
--------------------------------------------------------------------------------
