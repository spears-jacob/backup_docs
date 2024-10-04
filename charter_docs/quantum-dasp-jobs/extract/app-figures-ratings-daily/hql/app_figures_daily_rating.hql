USE ${env:DASP_db};

--------------------------------------------------------------------------------
------------------------- *** Construct variables *** --------------------------
--------------------------------------------------------------------------------

SET date_lag = 15;
set hive.exec.dynamic.partition.mode=nonstrict;


SELECT CONCAT('

***** END Construct variables *****

date_lag = ',${hiveconf:date_lag},'

')
;

--------------------------------------------------------------------------------
---------------- *** Split running total into daily counts *** -----------------
--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE ${env:DASP_db}.asp_app_figures_star_ratings_daily PARTITION (product_id,platform,version,partition_date_denver)
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
FROM app_figures_downloads
WHERE (partition_date_denver BETWEEN DATE_SUB('${hiveconf:RUN_DATE}',${hiveconf:date_lag})
                             AND '${hiveconf:RUN_DATE}')
AND product_id in ('40423315838','40425298890')
ORDER BY  platform,
          version,
          partition_date_denver
;

SELECT '

***** END Split running total into daily counts *****

'
;
