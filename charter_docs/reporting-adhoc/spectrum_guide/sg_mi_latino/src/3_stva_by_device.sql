SET BEGIN_DATE = '2017-10-01';
SET END_DATE = '2017-10-31';

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
                denver_date BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
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
                denver_date BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
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
