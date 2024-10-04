--Purpose: pull metrics for new sg markets
SET BEGIN_DATE = '2018-01-05';
SET END_DATE = '2018-02-06';

DROP TABLE IF EXISTS test.sag_active_on_zodiac;
CREATE TABLE test.sag_active_on_zodiac AS
SELECT
      partition_date_denver,
      account AS account__number_aes256,
      DATEDIFF(partition_date_denver, deployed_date) AS days_post_deployment

FROM prod.FILTERED_ZODIAC_ACTIVE_MACS
WHERE
          UPPER(type)='SUBSCRIBER'
      AND UPPER(customer__type)='RESIDENTIAL'
      AND partition_date_denver BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
GROUP BY
      partition_date_denver,
      account,
      DATEDIFF(partition_date_denver, deployed_date)
;

DROP TABLE IF EXISTS test.sag_active_on_venona;
CREATE TABLE test.sag_active_on_venona AS
SELECT
      partition_date_denver,
      account AS account__number_aes256,
      DATEDIFF(partition_date_denver, deployed_date) AS days_post_deployment

FROM prod.SP2_ACTIVE_MACS
WHERE
          UPPER(type)='SUBSCRIBER'
      AND UPPER(customer__type)='RESIDENTIAL'
      AND partition_date_denver BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
GROUP BY
      partition_date_denver,
      account,
      DATEDIFF(partition_date_denver, deployed_date)
;

DROP TABLE IF EXISTS test.sag_venona_errors;
CREATE TABLE test.sag_venona_errors AS
SELECT
      partition_date_denver,
      account__number_aes256,
      DATEDIFF(partition_date_denver, deployed_date) AS days_post_deployment,
      SUM(CASE WHEN metric_name REGEXP '.*(0220|0990|3001|3002|3006|3007|3009|3010|3012|3013|3014|3015|3016|3019|3021|3022|3030|9003).*' THEN event_counts ELSE 0 END) vod_error_count,
      SUM(CASE WHEN metric_name REGEXP '.*(0002|0006|0007|0008|0009|0010|0011|2001|2002|2003|2004|6001|6002|6003|7002|9001|9002).*'  THEN event_counts ELSE 0 END) guide_error_count,
      SUM(CASE WHEN metric_name REGEXP '.*(8001|8002|8003|8004|8005|8006|8007|8008|8009).*' THEN event_counts ELSE 0 END) dvr_error_count
FROM prod.VENONA_SG_P2_ERRORS_PAGEVIEWS_SEARCHS_BY_ACCOUNT_DAILY
WHERE
          category_type='error'
      AND UPPER(account_type)='SUBSCRIBER'
      AND UPPER(customer__type)='RESIDENTIAL'
      AND partition_date_denver BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
GROUP BY
      partition_date_denver,
      account__number_aes256,
      DATEDIFF(partition_date_denver, deployed_date)
;

DROP TABLE IF EXISTS test.sag_venona_pageviews;
CREATE TABLE test.sag_venona_pageviews AS
SELECT
      partition_date_denver,
      account__number_aes256,
      DATEDIFF(partition_date_denver, deployed_date) AS days_post_deployment,
      SUM(CASE WHEN UPPER(metric_name) IN ('VIDEOSTORE','TVSHOWS','MOVIES', 'CURATEDMOVIES', 'CURATEDTVSHOWS', 'CURATEDVIDEOSTORE') THEN event_counts ELSE 0 END) vod_pageview_count,
      SUM(CASE WHEN UPPER(metric_name) IN ('DVRHISTORY','DVRRECORDINGS','DVRSCHEDULED','DVRSERIESPRIORITY')   THEN event_counts ELSE 0 END) dvr_pageview_count
FROM prod.VENONA_SG_P2_ERRORS_PAGEVIEWS_SEARCHS_BY_ACCOUNT_DAILY
WHERE
          category_type='pageview'
      AND partition_date_denver BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
      AND UPPER(metric_name) IN
      ('SEARCH', 'VIDEOSTORE','TVSHOWS','MOVIES','MYLIBRARY','GUIDE', 'CURATEDMOVIES', 'CURATEDTVSHOWS', 'CURATEDVIDEOSTORE',
      'DVRHISTORY','DVRRECORDINGS','DVRSCHEDULED','DVRSERIESPRIORITY')
GROUP BY
      partition_date_denver,
      account__number_aes256,
      DATEDIFF(partition_date_denver, deployed_date)
;



DROP TABLE IF EXISTS test.sag_zodiac_dvr_usage;
CREATE TABLE test.sag_zodiac_dvr_usage AS
SELECT
      partition_date_denver,
      account__number_aes256,
      DATEDIFF(partition_date_denver, deployed_date) AS days_post_deployment,
      SUM(CASE WHEN dvr_action = 'record' THEN elapsed_seconds ELSE 0 END) AS record_elapsed_seconds_any_successful,
      SUM(CASE WHEN dvr_action = 'record' THEN count_of_events ELSE 0 END) AS record_count_of_events_any_successful,
      SUM(CASE WHEN dvr_action = 'play' THEN elapsed_seconds ELSE 0 END) AS play_elapsed_seconds_any_successful,
      SUM(CASE WHEN dvr_action = 'play' THEN count_of_events ELSE 0 END) AS play_count_of_events_any_successful,
      SUM(CASE WHEN dvr_action = 'schedule' THEN elapsed_seconds ELSE 0 END) AS schedule_elapsed_seconds_any_successful,
      SUM(CASE WHEN dvr_action = 'schedule' THEN count_of_events ELSE 0 END) AS schedule_count_of_events_any_successful

FROM prod.zodiac_dvr_by_account_agg_daily
WHERE
          partition_date_denver BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
      AND source = 'any'
      AND status = 'successful'
GROUP BY
      partition_date_denver,
      account__number_aes256,
      DATEDIFF(partition_date_denver, deployed_date)
;

DROP TABLE IF EXISTS test.sag_ad_adhoc_account_usage;
CREATE TABLE test.sag_ad_adhoc_account_usage AS
SELECT
      deployed.run_date,
      deployed.system__kma_desc,
      deployed.system__controller_name,
      deployed.service__category,
      deployed.account__type,
      deployed.customer__type,
      deployed.deployed_date,
      DATEDIFF(deployed.run_date,deployed.deployed_date) AS days_post_deployment,
      ah.product__is_spectrum_guide,
      serv.serv_cd,
      SIZE(COLLECT_SET(deployed.account__number_aes256)) AS hhs,
      SIZE(COLLECT_SET(CASE WHEN venona_active.account__number_aes256 IS NOT NULL THEN deployed.account__number_aes256 END)) AS hhs_venona,
      SIZE(COLLECT_SET(CASE WHEN zodiac_active.account__number_aes256 IS NOT NULL THEN deployed.account__number_aes256 END)) AS hhs_zodiac,
      SUM(total_view_duration_in_s) AS total_view_duration_in_s,
      SUM(free_view_duration_in_s) AS free_view_duration_in_s,
      SUM(trans_view_duration_in_s) AS trans_view_duration_in_s,
      SUM(prem_view_duration_in_s) AS prem_view_duration_in_s,
      SUM(total_number_of_views) AS total_number_of_views,
      SUM(free_views) AS free_views,
      SUM(trans_views) AS trans_views,
      SUM(prem_views) AS prem_views,
      SUM(vod_error_count) AS vod_error_count,
      SUM(guide_error_count) AS guide_error_count,
      SUM(dvr_error_count) AS dvr_error_count,
      SUM(record_elapsed_seconds_any_successful) AS record_elapsed_seconds_any_successful,
      SUM(record_count_of_events_any_successful) AS record_count_of_events_any_successful,
      SUM(play_elapsed_seconds_any_successful) AS play_elapsed_seconds_any_successful,
      SUM(play_count_of_events_any_successful) AS play_count_of_events_any_successful,
      SUM(schedule_elapsed_seconds_any_successful) AS schedule_elapsed_seconds_any_successful,
      SUM(schedule_count_of_events_any_successful) AS schedule_count_of_events_any_successful,
      SUM(vod_pageview_count) AS vod_pageview_count,
      SUM(dvr_pageview_count) AS dvr_pageview_count
FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY deployed
LEFT JOIN prod.account_history ah
      ON deployed.account__number_aes256 = ah.account__number_aes256
      AND deployed.run_date = ah.partition_date_time
LEFT JOIN prod.account_serv_cd_history serv
      ON deployed.run_date = serv.partition_date_time
      AND deployed.account__number_aes256 = serv.account_number_aes256
LEFT JOIN prod.VOD_ACCT_LEVEL_USAGE_DAILY_AGG VOD
      ON deployed.run_date = vod.partition_date_time
      AND deployed.account__number_aes256 = vod.account__number_aes256
LEFT JOIN test.sag_venona_errors venona_errors
      ON deployed.run_date = venona_errors.partition_date_denver
      AND deployed.account__number_aes256 = venona_errors.account__number_aes256
LEFT JOIN test.sag_venona_pageviews venona_pageviews
      ON deployed.run_date = venona_pageviews.partition_date_denver
      AND deployed.account__number_aes256 = venona_pageviews.account__number_aes256
LEFT JOIN test.sag_zodiac_dvr_usage zodiac_dvr
      ON deployed.run_date = zodiac_dvr.partition_date_denver
      AND deployed.account__number_aes256 = zodiac_dvr.account__number_aes256
LEFT JOIN test.sag_active_on_zodiac zodiac_active
      ON deployed.run_date = zodiac_active.partition_date_denver
      AND deployed.account__number_aes256 = zodiac_active.account__number_aes256
LEFT JOIN test.sag_active_on_venona venona_active
      ON deployed.run_date = venona_active.partition_date_denver
      AND deployed.account__number_aes256 = venona_active.account__number_aes256
WHERE
          deployed.run_date BETWEEN ${hiveconf:BEGIN_DATE} AND ${hiveconf:END_DATE}
      AND deployed.account__type IN ('SUBSCRIBER')
      AND deployed.customer__type = 'Residential'
      AND deployed.customer__disconnect_date IS NULL
      AND (deployed.system__kma_desc IN
                (
                'Southern New England',
                'Pacific Northwest',
                'TN / LA / AL'
              )
      OR (deployed.system__kma_desc IN ('Greenville, SC / Georgia') AND deployed.system__controller_name IN ('GSA'))
    )



GROUP BY
      deployed.run_date,
      deployed.system__kma_desc,
      deployed.system__controller_name,
      deployed.service__category,
      deployed.account__type,
      deployed.customer__type,
      deployed.deployed_date,
      ah.product__is_spectrum_guide,
      DATEDIFF(deployed.run_date,deployed.deployed_date),
      serv.serv_cd
;



hive -e "set hive.cli.print.header=true; SELECT
  *
  FROM test.sag_ad_adhoc_account_usage
  ;" > ad_hoc_account_pull_20180209_for_laura.txt


lftp sftp://spectrum_fl:M2LmBttr@files.chartercom.com -e " cd customer_care_sg; put -c ad_hoc_account_pull_20180209_for_laura.txt; bye"
