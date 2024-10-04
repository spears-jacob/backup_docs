#!/bin/bash

echo "Dropping Views"
hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_msa_version_hist;"
hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_msa_version_labels;"
hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_msa_version_unified;"
hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_msa_version_hist_ratings;"
hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_msa_version_label_ratings;"
hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_afd_daily_ratings;"
hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_msa_version_ratings_unified;"
hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_app_ratings_daily_average;"

echo "Creating Views"

hive -v -e "CREATE VIEW ${DASP_db}.asp_v_app_figures_downloads AS SELECT * from ${DASP_db}.app_figures_downloads;"

hive -v -e "CREATE VIEW ${DASP_db}.asp_v_app_figures_sentiment AS SELECT * from ${DASP_db}.app_figures_sentiment;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_msa_version_hist AS
select  ROW_NUMBER() OVER (PARTITION BY platform ORDER BY platform DESC,version DESC) AS rownum,
        version,
        store,
        platform,
        SUM(ct_value) AS sum_value
FROM
      (  select  version,
                partition_date_denver,
                store,
                CASE
                  WHEN store = 'apple' THEN 'iOS'
                  WHEN store = 'google_play' THEN 'Android'
                  ELSE NULL END AS platform,
                COUNT(*) as ct_value
        FROM  ${DASP_db}.app_figures_sentiment
        where product_id in ('40423315838','40425298890')
        GROUP BY  store,
                  version,
                  partition_date_denver
      ) a
GROUP BY  platform,
          version,
          store
ORDER BY  platform DESC,
          version DESC
;

SELECT '

***** END Set Recency Rankings For Versions *****

'
;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_msa_version_labels AS
select  platform,
        store,
        version,
        rownum
FROM ${DASP_db}.asp_v_msa_version_hist
WHERE rownum IN (1,2,3,4)
;

SELECT '

***** END Most Recent Reviews Ranked *****

'
;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_msa_version_unified AS
select  CASE
          WHEN afs.store = 'apple' THEN 'iOS'
          WHEN afs.store = 'google_play' THEN 'Android'
          ELSE NULL END AS platform,
        afs.version AS version,
        CASE
          WHEN vls.rownum = 1 THEN 'rank1'
          WHEN vls.rownum = 2 THEN 'rank2'
          WHEN vls.rownum = 3 THEN 'rank3'
          WHEN vls.rownum = 4 THEN 'rank4'
          ELSE 'rank5' END AS recency_rank,
        CASE
          WHEN vls.version IS NOT NULL THEN vls.version
          ELSE 'older_version'
          END AS grouped_version,
        afs.author,
        afs.title,
        afs.review,
        afs.original_title,
        afs.original_review,
        afs.stars,
        afs.iso,
        afs.product_name,
        afs.product_id,
        afs.vendor_id,
        afs.weight,
        afs.id,
        afs.language,
        afs.sentiment,
        afs.partition_date_denver
FROM ${DASP_db}.app_figures_sentiment afs
LEFT JOIN asp_v_msa_version_labels vls
  ON vls.store = afs.store
    AND vls.version = afs.version
WHEre afs.product_id in ('40423315838','40425298890')
;

SELECT '

***** END Reviews With Recency Rankings *****

'
;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_msa_version_hist_ratings AS
select  ROW_NUMBER() OVER (PARTITION BY platform ORDER BY platform DESC,version DESC) AS rownum,
        version,
        store,
        platform,
        SUM(ct_value) AS sum_value
FROM  ( select  version,
                partition_date_denver,
                store,
                CASE
                  WHEN store = 'apple' THEN 'iOS'
                  WHEN store = 'google_play' THEN 'Android'
                  ELSE NULL END AS platform,
                COUNT(*) as ct_value
        FROM ${DASP_db}.app_figures_downloads
        where product_id in ('40423315838','40425298890')
        AND partition_date_denver >= '2017-01-01'
        GROUP BY  store,
                  version,
                  partition_date_denver
      ) a
GROUP BY  platform,
          version,
          store
ORDER BY  platform DESC,
          version DESC
;

SELECT '

***** END Set Recency Rankings For Versions *****

'
;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_msa_version_label_ratings AS
select  platform,
        store,
        version,
        rownum
FROM ${DASP_db}.asp_v_msa_version_hist_ratings
WHERE rownum IN (1,2,3,4)
;


SELECT '

***** END Create ranks for most recent 4 versions, bucket older *****

'
;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_afd_daily_ratings AS
select  platform,
        product_name,
        product_id,
        version,
        star_rating,
        rating_count,
        partition_date_denver
FROM
  ( SELECT  platform,
            product_name,
            product_id,
            version,
            CASE
                WHEN metric = 'one_star_daily' THEN CAST(1.00 AS DECIMAL(10,2))
                WHEN metric = 'two_star_daily' THEN CAST(2.00 AS DECIMAL(10,2))
                WHEN metric = 'thr_star_daily' THEN CAST(3.00 AS DECIMAL(10,2))
                WHEN metric = 'fou_star_daily' THEN CAST(4.00 AS DECIMAL(10,2))
                WHEN metric = 'fiv_star_daily' THEN CAST(5.00 AS DECIMAL(10,2))
            ELSE CAST(NULL AS DECIMAL(10,2))
            END AS star_rating,
            rating_count,
            partition_date_denver
    FROM  (SELECT platform,
                  product_name,
                  product_id,
                  version,
                  partition_date_denver,
                  MAP(
                    'one_star_daily',one_star_daily,
                    'two_star_daily',two_star_daily,
                    'thr_star_daily',thr_star_daily,
                    'fou_star_daily',fou_star_daily,
                    'fiv_star_daily',fiv_star_daily
                  ) as rating_counts
              FROM ${DASP_db}.asp_app_figures_star_ratings_daily
              WHERE partition_date_denver >= '2017-01-01'
            ) as rating
            LATERAL VIEW EXPLODE (rating_counts) exploded_table AS metric,rating_count
          ) ratings
WHERE rating_count IS NOT NULL
;

SELECT '

***** END Group daily star counts by star rating into one field *****

'
;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_msa_version_ratings_unified AS
select  afd.platform,
        afd.product_name,
        afd.product_id,
        afd.version,
        CASE
          WHEN vls.version IS NOT NULL THEN vls.version
          ELSE 'older_version'
          END AS grouped_version,
        afd.star_rating,
        afd.rating_count,
        CASE
          WHEN vls.rownum = 1 THEN 'rank1'
          WHEN vls.rownum = 2 THEN 'rank2'
          WHEN vls.rownum = 3 THEN 'rank3'
          WHEN vls.rownum = 4 THEN 'rank4'
          ELSE 'rank5' END AS recency_rank,
        afd.partition_date_denver
FROM ${DASP_db}.asp_v_afd_daily_ratings afd
LEFT JOIN asp_v_msa_version_label_ratings vls
  ON vls.platform = afd.platform
    AND vls.version = afd.version
;

SELECT '

***** END Unify rankings and daily views *****

'
;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_app_ratings_daily_average AS
select    partition_date_denver,
          platform,
          CAST((
            one_star_to_date +
            two_star_to_date +
            thr_star_to_date +
            fou_star_to_date +
            fiv_star_to_date
          ) AS BIGINT)
          AS total_reviews_to_date,
          CAST((
              (1 * one_star_to_date) +
              (2 * two_star_to_date) +
              (3 * thr_star_to_date) +
              (4 * fou_star_to_date) +
              (5 * fiv_star_to_date)
            ) AS BIGINT)
            AS total_weight_per_day,
          CAST((
              (1 * one_star_to_date) +
              (2 * two_star_to_date) +
              (3 * thr_star_to_date) +
              (4 * fou_star_to_date) +
              (5 * fiv_star_to_date)
            )
            /
            (
              one_star_to_date +
              two_star_to_date +
              thr_star_to_date +
              fou_star_to_date +
              fiv_star_to_date
            ) AS DECIMAL(10,2))
            AS weighted_average_per_day
FROM ${DASP_db}.asp_app_figures_star_ratings_daily
;


SELECT '

***** END Weighted daily average rating *****

'
;"