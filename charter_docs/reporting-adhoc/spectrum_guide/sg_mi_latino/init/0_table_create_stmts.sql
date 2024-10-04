-- ============
-- == LATINO VIDEO PACKAGES
-- ============
DROP TABLE IF EXISTS test.vod_latino_base;
CREATE TABLE IF NOT EXISTS test.vod_latino_base
(
    system__kma_desc                    string,
    account__number_aes256              string,
    product__is_spectrum_guide          string,
    account__category                   string,
    product__video_package_type         string,
    title__content_provider_name        string,
    total_number_of_views               bigint,
    seconds_viewed                      bigint

)
PARTITIONED BY (
    partition_year_month string)
STORED AS ORC
TBLPROPERTIES (
    'ORC.COMPRESS'='SNAPPY',
    'ORC.COMPRESS.SIZE'='8192');


DROP TABLE IF EXISTS test.latino_vod_sg_venona_base_metrics_agg;
CREATE TABLE IF NOT EXISTS test.latino_vod_sg_venona_base_metrics_agg
(
      product__is_spectrum_guide                string,
      product__video_package_type               string,
      system__kma_desc                          string,
      account__category                         string,
      -- stva_usage_flag                           string,
      -- application_type                          string,
      -- playback_type                             string,
      account__number_aes256                    string,
      stb_content_views                         bigint,
      stb_seconds_viewed                        bigint,
      stva_mili_seconds_viewed                  bigint
)
PARTITIONED BY (
    partition_year_month string)
STORED AS ORC
TBLPROPERTIES (
    'ORC.COMPRESS'='SNAPPY',
    'ORC.COMPRESS.SIZE'='8192');


DROP TABLE IF EXISTS test.latino_vod_sg_venona_base_metrics_agg_by_device;
CREATE TABLE IF NOT EXISTS test.latino_vod_sg_venona_base_metrics_agg_by_device
(
      product__is_spectrum_guide                string,
      product__video_package_type               string,
      system__kma_desc                          string,
      account__category                         string,
      stva_usage_flag                           string,
      application_type                          string,
      playback_type                             string,
      account__number_aes256                    string,
      -- stb_content_views                         bigint,
      -- stb_seconds_viewed                        bigint,
      stva_mili_seconds_viewed                  bigint
)
PARTITIONED BY (
    partition_year_month string)
STORED AS ORC
TBLPROPERTIES (
    'ORC.COMPRESS'='SNAPPY',
    'ORC.COMPRESS.SIZE'='8192');



-- ============
-- == ALL VIDEO PACKAGES
-- ============

DROP TABLE IF EXISTS test.vod_all_packages_base;
CREATE TABLE IF NOT EXISTS test.vod_all_packages_base
(
    system__kma_desc                    string,
    account__number_aes256              string,
    product__is_spectrum_guide          string,
    account__category                   string,
    product__video_package_type         string,
    title__content_provider_name        string,
    total_number_of_views               bigint,
    seconds_viewed                      bigint

)
PARTITIONED BY (
    partition_year_month string)
STORED AS ORC
TBLPROPERTIES (
    'ORC.COMPRESS'='SNAPPY',
    'ORC.COMPRESS.SIZE'='8192');




DROP TABLE IF EXISTS test.vod_sg_venona_base_metrics_agg;
CREATE TABLE IF NOT EXISTS test.vod_sg_venona_base_metrics_agg
(
      product__is_spectrum_guide                string,
      product__video_package_type               string,
      system__kma_desc                          string,
      account__category                         string,
      -- stva_usage_flag                           string,
      -- application_type                          string,
      -- playback_type                             string,
      account__number_aes256                    string,
      stb_content_views                         bigint,
      stb_seconds_viewed                        bigint,
      stva_mili_seconds_viewed                  bigint
)
PARTITIONED BY (
    partition_year_month string)
STORED AS ORC
TBLPROPERTIES (
    'ORC.COMPRESS'='SNAPPY',
    'ORC.COMPRESS.SIZE'='8192')
    ;


DROP TABLE IF EXISTS test.vod_sg_venona_base_metrics_agg_by_device;
CREATE TABLE IF NOT EXISTS test.vod_sg_venona_base_metrics_agg_by_device
(
      product__is_spectrum_guide                string,
      product__video_package_type               string,
      system__kma_desc                          string,
      account__category                         string,
      stva_usage_flag                           string,
      application_type                          string,
      playback_type                             string,
      account__number_aes256                    string,
      -- stb_content_views                         bigint,
      -- stb_seconds_viewed                        bigint,
      stva_mili_seconds_viewed                  bigint
)
PARTITIONED BY (
    partition_year_month string)
STORED AS ORC
TBLPROPERTIES (
    'ORC.COMPRESS'='SNAPPY',
    'ORC.COMPRESS.SIZE'='8192')
    ;
