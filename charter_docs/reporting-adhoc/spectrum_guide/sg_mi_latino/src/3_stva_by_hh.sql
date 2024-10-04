SET BEGIN_DATE = '2017-07-01';


-- ============
-- == STVA: ALL VIDEO PACKAGES
-- ============
-- ============
-- == STEP 1: DISTINCT LIST OF LEGACY AND SG CUSTOMER VOD USAGE
-- ============
SELECT partition_year_month,product__is_spectrum_guide,product__video_package_type,account__category, SUM(stb_seconds_viewed)/3600,  SIZE(COLLECT_SET(account__number_aes256))
FROM test.vod_sg_base
GROUP BY partition_year_month,product__is_spectrum_guide,product__video_package_type,account__category;

SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

DROP TABLE IF EXISTS test.vod_sg_base;
CREATE TABLE test.vod_sg_base AS

SELECT
      vod.partition_year_month,
      vod.system__kma_desc,
      vod.product__is_spectrum_guide,
      vod.account__category,
      vod.product__video_package_type,
      vod.account__number_aes256,
      SUM(total_number_of_views) AS stb_total_views,
      SUM(vod.seconds_viewed) AS stb_seconds_viewed
FROM test.vod_all_packages_base vod
WHERE partition_year_month = SUBSTRING(${hiveconf:BEGIN_DATE}, 0 , 7)
GROUP BY
      vod.partition_year_month,
      vod.system__kma_desc,
      vod.product__is_spectrum_guide,
      vod.product__video_package_type,
      vod.account__category,
      vod.account__number_aes256
;

-- ============
-- == STEP 2: JOIN BI SG BASE TO VENONA FOR STVA USAGE METRICS
-- ============

SELECT partition_year_month,product__is_spectrum_guide,product__video_package_type,account__category, SUM(stb_seconds_viewed)/3600,  SIZE(COLLECT_SET(account__number_aes256))
FROM test.vod_sg_venona_base_metrics
GROUP BY partition_year_month,product__is_spectrum_guide,product__video_package_type,account__category;


SET hive.auto.convert.join=false;

DROP TABLE IF EXISTS test.vod_sg_venona_base_metrics;
CREATE TABLE IF NOT EXISTS test.vod_sg_venona_base_metrics AS
SELECT
      bi.partition_year_month,
      bi.system__kma_desc,
      bi.product__is_spectrum_guide,
      bi.product__video_package_type,
      bi.account__category,
      -- IF(venona.billing_id IS NOT NULL, 'Y', 'N') AS stva_usage_flag,
      bi.account__number_aes256,
      venona.watch_time_ms AS watch_time_ms,
      SUM(bi.stb_total_views) AS stb_total_views,
      SUM(bi.stb_seconds_viewed) AS stb_seconds_viewed
FROM test.vod_sg_base bi
INNER JOIN
      (
      SELECT
            SUBSTRING(denver_date, 0, 7) AS partition_year_month,
            denver_date,
            billing_id,
            SUM(watch_time_ms) AS watch_time_ms
      FROM prod.venona_metric_agg
      WHERE
                denver_date BETWEEN ${hiveconf:BEGIN_DATE} AND LAST_DAY(${hiveconf:BEGIN_DATE})
            AND playback_type = 'vod'
      GROUP BY
            SUBSTRING(denver_date, 0, 7),
            denver_date,
            billing_id
      ) venona
ON venona.billing_id = prod.aes_encrypt(prod.aes_decrypt256(bi.account__number_aes256))
AND venona.partition_year_month = bi.partition_year_month

GROUP BY
    bi.partition_year_month,
    bi.system__kma_desc,
    bi.product__is_spectrum_guide,
    bi.product__video_package_type,
    bi.account__category,
    bi.account__number_aes256,
    venona.watch_time_ms
;

-- ============
-- == STEP 3: BUILD VOD SG AND STVA AGG
-- ============
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=3368709120;
SET mapreduce.input.fileinputformat.split.minsize=3368709120;

INSERT OVERWRITE TABLE test.vod_sg_venona_base_metrics_agg PARTITION(partition_year_month)

SELECT
      product__is_spectrum_guide,
      product__video_package_type,
      system__kma_desc,
      account__category,
      account__number_aes256,
      SUM(stb_total_views) AS stb_content_views,
      SUM(stb_seconds_viewed) AS stb_seconds_viewed,
      SUM(watch_time_ms) AS stva_mili_seconds_viewed,
      partition_year_month
FROM test.vod_sg_venona_base_metrics
GROUP BY
      product__is_spectrum_guide,
      product__video_package_type,
      system__kma_desc,
      account__category,
      account__number_aes256,
      partition_year_month
;



-- ============
-- == STVA: LATINO PACKAGES
-- ============
-- ============
-- == STEP 1: DISTINCT LIST OF LEGACY AND SG CUSTOMER VOD USAGE
-- ============

SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;

DROP TABLE IF EXISTS test_tmp.latino_vod_sg_base;
CREATE TABLE test_tmp.latino_vod_sg_base AS

SELECT
      vod.partition_year_month,
      vod.system__kma_desc,
      vod.product__is_spectrum_guide,
      vod.account__category,
      vod.product__video_package_type,
      vod.account__number_aes256,
      SUM(total_number_of_views) AS stb_total_views,
      SUM(vod.seconds_viewed) AS stb_seconds_viewed
FROM test.vod_latino_base vod
WHERE partition_year_month = SUBSTRING(${hiveconf:BEGIN_DATE}, 0 , 7)
GROUP BY
      vod.partition_year_month,
      vod.system__kma_desc,
      vod.product__is_spectrum_guide,
      vod.product__video_package_type,
      vod.account__category,
      vod.account__number_aes256
;

-- ============
-- == STEP 2: JOIN BI SG BASE TO VENONA FOR STVA USAGE METRICS
-- ============
SET hive.auto.convert.join=false;

DROP TABLE IF EXISTS test_tmp.latino_vod_sg_venona_base_metrics_agg;
CREATE TABLE IF NOT EXISTS test_tmp.latino_vod_sg_venona_base_metrics_agg AS
SELECT
      bi.partition_year_month,
      bi.system__kma_desc,
      bi.product__is_spectrum_guide,
      bi.product__video_package_type,
      bi.account__category,
      bi.account__number_aes256,
      SUM(bi.stb_total_views) AS stb_total_views,
      SUM(bi.stb_seconds_viewed) AS stb_seconds_viewed,
      SUM(venona.watch_time_ms) AS watch_time_ms
FROM test_tmp.latino_vod_sg_base bi
INNER JOIN
      (
      SELECT
            SUBSTRING(denver_date, 0, 7) AS partition_year_month,
            denver_date,
            billing_id,
            -- application_type,
            -- playback_type,
            watch_time_ms
      FROM prod.venona_metric_agg
      WHERE
                denver_date BETWEEN ${hiveconf:BEGIN_DATE} AND LAST_DAY(${hiveconf:BEGIN_DATE})
            AND playback_type = 'vod'
      ) venona
ON venona.billing_id = prod.aes_encrypt(prod.aes_decrypt256(bi.account__number_aes256))
AND venona.partition_year_month = bi.partition_year_month

GROUP BY
    bi.partition_year_month,
    bi.system__kma_desc,
    bi.product__is_spectrum_guide,
    bi.product__video_package_type,
    bi.account__category,
    bi.account__number_aes256

;

-- ============
-- == STEP 3: BUILD VOD SG AND STVA AGG
-- ============
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=3368709120;
SET mapreduce.input.fileinputformat.split.minsize=3368709120;

INSERT OVERWRITE TABLE test.latino_vod_sg_venona_base_metrics_agg PARTITION(partition_year_month)

SELECT
      product__is_spectrum_guide,
      product__video_package_type,
      system__kma_desc,
      account__category,
      account__number_aes256,
      SUM(stb_total_views) AS stb_content_views,
      SUM(stb_seconds_viewed) AS stb_seconds_viewed,
      SUM(watch_time_ms) AS stva_mili_seconds_viewed,
      partition_year_month
FROM test_tmp.latino_vod_sg_venona_base_metrics_agg
GROUP BY
      product__is_spectrum_guide,
      product__video_package_type,
      system__kma_desc,
      account__category,
      account__number_aes256,
      partition_year_month
;





-- ============
-- == STVA: ALL VIDEO PACKAGES BY DEVICES
-- ============
-- ============
-- == STEP 2: JOIN BI SG BASE TO VENONA FOR STVA USAGE METRICS
-- ============

SET hive.auto.convert.join=false;

DROP TABLE IF EXISTS test.vod_sg_venona_base_metrics_by_device;
CREATE TABLE IF NOT EXISTS test.vod_sg_venona_base_metrics_by_device AS
SELECT
      bi.partition_year_month,
      bi.system__kma_desc,
      bi.product__is_spectrum_guide,
      bi.product__video_package_type,
      bi.account__category,
      IF(venona.billing_id IS NOT NULL, 'Y', 'N') AS stva_usage_flag,
      bi.account__number_aes256,
      venona.billing_id,
      venona.application_type,
      venona.playback_type,
      venona.watch_time_ms
FROM test.vod_sg_base bi
INNER JOIN
      (
      SELECT
            SUBSTRING(denver_date, 0, 7) AS partition_year_month,
            denver_date,
            billing_id,
            application_type,
            playback_type,
            SUM(watch_time_ms) AS watch_time_ms
      FROM prod.venona_metric_agg
      WHERE
                denver_date BETWEEN ${hiveconf:BEGIN_DATE} AND LAST_DAY(${hiveconf:BEGIN_DATE})
            AND playback_type = 'vod'
      GROUP BY
            SUBSTRING(denver_date, 0, 7),
            denver_date,
            billing_id,
            application_type,
            playback_type
      ) venona
ON venona.billing_id = prod.aes_encrypt(prod.aes_decrypt256(bi.account__number_aes256))
AND venona.partition_year_month = bi.partition_year_month

GROUP BY
    bi.partition_year_month,
    bi.system__kma_desc,
    bi.product__is_spectrum_guide,
    bi.product__video_package_type,
    bi.account__category,
    IF(venona.billing_id IS NOT NULL, 'Y', 'N'),
    bi.account__number_aes256,
    venona.billing_id,
    venona.application_type,
    venona.playback_type,
    venona.watch_time_ms
;

-- ============
-- == STEP 3: BUILD VOD SG AND STVA AGG
-- ============
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=3368709120;
SET mapreduce.input.fileinputformat.split.minsize=3368709120;

INSERT OVERWRITE TABLE test.vod_sg_venona_base_metrics_agg_by_device PARTITION(partition_year_month)

SELECT
      product__is_spectrum_guide,
      product__video_package_type,
      system__kma_desc,
      account__category,
      stva_usage_flag,
      application_type,
      playback_type,
      account__number_aes256,
      SUM(watch_time_ms) AS stva_mili_seconds_viewed,
      partition_year_month
FROM test.vod_sg_venona_base_metrics_by_device
GROUP BY
      product__is_spectrum_guide,
      product__video_package_type,
      system__kma_desc,
      account__category,
      stva_usage_flag,
      application_type,
      playback_type,
      account__number_aes256,
      partition_year_month
;


-- ============
-- == STVA: LATINO PACKAGES BY DEVICE
-- ============

-- ============
-- == STEP 2: JOIN BI SG BASE TO VENONA FOR STVA USAGE METRICS
-- ============
SET hive.auto.convert.join=false;

DROP TABLE IF EXISTS test_tmp.latino_vod_sg_venona_base_metrics_agg_by_device;
CREATE TABLE IF NOT EXISTS test_tmp.latino_vod_sg_venona_base_metrics_agg_by_device AS
SELECT
      bi.partition_year_month,
      bi.system__kma_desc,
      bi.product__is_spectrum_guide,
      bi.product__video_package_type,
      bi.account__category,
      IF(venona.billing_id IS NOT NULL, 'Y', 'N') AS stva_usage_flag,
      bi.account__number_aes256,
      venona.billing_id,
      venona.application_type,
      venona.playback_type,
      venona.watch_time_ms
FROM test_tmp.latino_vod_sg_base bi
INNER JOIN
      (
      SELECT
            SUBSTRING(denver_date, 0, 7) AS partition_year_month,
            denver_date,
            billing_id,
            application_type,
            playback_type,
            SUM(watch_time_ms) AS watch_time_ms
      FROM prod.venona_metric_agg
      WHERE
                denver_date BETWEEN ${hiveconf:BEGIN_DATE} AND LAST_DAY(${hiveconf:BEGIN_DATE})
            AND playback_type = 'vod'
      GROUP BY
            SUBSTRING(denver_date, 0, 7),
            denver_date,
            billing_id,
            application_type,
            playback_type
      ) venona
ON venona.billing_id = prod.aes_encrypt(prod.aes_decrypt256(bi.account__number_aes256))
AND venona.partition_year_month = bi.partition_year_month

GROUP BY
    bi.partition_year_month,
    bi.system__kma_desc,
    bi.product__is_spectrum_guide,
    bi.product__video_package_type,
    bi.account__category,
    IF(venona.billing_id IS NOT NULL, 'Y', 'N'),
    bi.account__number_aes256,
    venona.billing_id,
    venona.application_type,
    venona.playback_type,
    venona.watch_time_ms
;

-- ============
-- == STEP 3: BUILD VOD SG AND STVA AGG
-- ============
SET hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapreduce.input.fileinputformat.split.maxsize=3368709120;
SET mapreduce.input.fileinputformat.split.minsize=3368709120;

INSERT OVERWRITE TABLE test.latino_vod_sg_venona_base_metrics_agg_by_device PARTITION(partition_year_month)

SELECT
      product__is_spectrum_guide,
      product__video_package_type,
      system__kma_desc,
      account__category,
      stva_usage_flag,
      application_type,
      playback_type,
      account__number_aes256,
      SUM(watch_time_ms) AS stva_mili_seconds_viewed,
      partition_year_month
FROM test_tmp.latino_vod_sg_venona_base_metrics_agg_by_device
GROUP BY
      product__is_spectrum_guide,
      product__video_package_type,
      system__kma_desc,
      account__category,
      stva_usage_flag,
      application_type,
      playback_type,
      account__number_aes256,
      partition_year_month
;
