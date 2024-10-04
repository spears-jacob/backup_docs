-- ============
-- == STVA + SG: UNIQUE HHS
-- ============
SELECT
      partition_year_month,
      product__video_package_type,
      product__is_spectrum_guide,
      system__kma_desc,
      account__category,
      SIZE(COLLECT_SET(account__number_aes256)) AS households,
      SUM(stb_seconds_viewed) AS stb_seconds_viewed,
      SUM(stva_mili_seconds_viewed) AS stva_mili_seconds_viewed
FROM test.vod_sg_venona_base_metrics_agg
GROUP BY
      partition_year_month,
      product__video_package_type,
      product__is_spectrum_guide,
      system__kma_desc,
      account__category

UNION ALL

SELECT
      partition_year_month,
      product__video_package_type,
      product__is_spectrum_guide,
      system__kma_desc,
      account__category,
      SIZE(COLLECT_SET(account__number_aes256)) AS households,
      SUM(stb_seconds_viewed) AS stb_seconds_viewed,
      SUM(stva_mili_seconds_viewed) AS stva_mili_seconds_viewed
FROM test.latino_vod_sg_venona_base_metrics_agg
GROUP BY
      partition_year_month,
      product__video_package_type,
      product__is_spectrum_guide,
      system__kma_desc,
      account__category

-- ============
-- == STVA + SG Accounts by Device Type
-- ============
SELECT
      partition_year_month,
      product__video_package_type,
      product__is_spectrum_guide,
      system__kma_desc,
      account__category,
      application_type,
      SIZE(COLLECT_SET(account__number_aes256)) AS households,
      SUM(stva_mili_seconds_viewed) AS stva_mili_seconds_viewed
FROM test.vod_sg_venona_base_metrics_agg_by_device
GROUP BY
      partition_year_month,
      product__video_package_type,
      product__is_spectrum_guide,
      system__kma_desc,
      account__category,
      application_type
UNION ALL
SELECT
      partition_year_month,
      product__video_package_type,
      product__is_spectrum_guide,
      system__kma_desc,
      account__category,
      application_type,
      SIZE(COLLECT_SET(account__number_aes256)) AS households,
      SUM(stva_mili_seconds_viewed) AS stva_mili_seconds_viewed
FROM test.latino_vod_sg_venona_base_metrics_agg_by_device
GROUP BY
      partition_year_month,
      product__video_package_type,
      product__is_spectrum_guide,
      system__kma_desc,
      account__category,
      application_type
      ;

-- ============
-- == SG:VOD USAGE PER PACKAGE -- MUST APPEAR IN EVENTS
-- ============
SELECT
      partition_year_month,
      system__kma_desc,
      account__category,
      product__is_spectrum_guide,
      product__video_package_type,
      SIZE(COLLECT_SET(account__number_aes256)) AS households,
      SUM(seconds_viewed) AS  seconds_viewed,
      SUM(total_number_of_views) AS content_views
FROM test.vod_all_packages_base
GROUP BY
      partition_year_month,
      system__kma_desc,
      account__category,
      product__is_spectrum_guide,
      product__video_package_type

UNION ALL

SELECT
      partition_year_month,
      system__kma_desc,
      account__category,
      product__is_spectrum_guide,
      product__video_package_type,
      SIZE(COLLECT_SET(account__number_aes256)) AS households,
      SUM(seconds_viewed) AS  seconds_viewed,
      SUM(total_number_of_views) AS content_views
FROM test.vod_latino_base
GROUP BY
      partition_year_month,
      system__kma_desc,
      account__category,
      product__is_spectrum_guide,
      product__video_package_type
      ;


--TOP NETWORKS

DROP TABLE IF EXISTS test.vod_top_networks;
CREATE TABLE test.vod_top_networks AS
SELECT
      partition_year_month,
      system__kma_desc,
      account__category,
      product__is_spectrum_guide,
      product__video_package_type,
      title__content_provider_name,
      SIZE(COLLECT_SET(account__number_aes256)) AS households,
      SUM(seconds_viewed) AS  seconds_viewed,
      SUM(total_number_of_views) AS content_views
FROM test.vod_all_packages_base
GROUP BY
      partition_year_month,
      system__kma_desc,
      account__category,
      product__is_spectrum_guide,
      product__video_package_type,
      title__content_provider_name

UNION ALL

SELECT
      partition_year_month,
      system__kma_desc,
      account__category,
      product__is_spectrum_guide,
      product__video_package_type,
      title__content_provider_name,
      SIZE(COLLECT_SET(account__number_aes256)) AS households,
      SUM(seconds_viewed) AS  seconds_viewed,
      SUM(total_number_of_views) AS content_views
FROM test.vod_latino_base
GROUP BY
      partition_year_month,
      system__kma_desc,
      account__category,
      product__is_spectrum_guide,
      product__video_package_type,
      title__content_provider_name

;
