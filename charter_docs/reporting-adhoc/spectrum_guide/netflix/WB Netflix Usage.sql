-------
-- 1. Staging Table for WB Accounts and Cloud Activity
-------
set mapreduce.input.fileinputformat.split.minsize=5368709120;
set mapreduce.input.fileinputformat.split.maxsize=5368709120;
set hive.tez.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

DROP TABLE IF EXISTS dev.brian_wb_sg_activity;

CREATE TABLE dev.brian_wb_sg_activity AS 
SELECT prod.epoch_converter(received__timestamp ,'America/Denver') AS partition_date_denver,
       b.account__number_aes256 AS account__number_aes256,
       visit__application_details__app_version,
       visit__application_details__application_name,
       message__name,
       state__view__current_page__page_name,
       state__view__current_page__elements__standardized_name,
       state__content__identifiers__tms_guide_id
       FROM prod.venona_events events
       JOIN
         (SELECT run_date,
                 account__mac_id_aes256,
                 account__number_aes256
            FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
           WHERE run_date >= '2018-02-12'
             AND equipment__model RLIKE '^SP'
             AND account__type = 'SUBSCRIBER'
             AND customer__type = 'Residential'
             AND system__controller_name = 'NEWTOWN'  
             AND account__number_aes256 NOT IN  ('CQgl7Bf4I3HiVMt3ADHsDCWVBt5umlcZMb8qqzNbvcw=',
                                                 'FcIGT0rW/nxyVV8D/hsDMKzABDbcXjZrIZ3xRbOdkEs=',
                                                 'ZUnZ49j6tS4bt5EaxFcaWAP1lGfKdyESpqiZpfNzI/E=',
                                                 'fXn13Tz9B8ot6EzwqnpAQOjpVWuqeIvTYaKBWBE2Xak=',
                                                 'wAsUbC2d8Gp4oouQowPBlSqyNJsQEoI3p/aIqkYtAFQ=') -- exclude currently known account__dwelling_description = 'Residential Test Account' accounts until account ETL is updated
) AS b
          ON lower(prod.aes_decrypt(events.visit__device__uuid)) = lower(prod.aes_decrypt256 (b.account__mac_id_aes256))
         AND prod.epoch_converter(events.received__timestamp ,'America/Denver') = b.run_date
       WHERE partition_date_utc >= '2018-02-11'
         AND visit__application_details__application_name = 'Spectrum Guide';

---------
-- Query
---------
SELECT deployed_count.run_date AS run_date,
       deployed_count.wb_deployed_count AS wb_deployed_count,
       active_count.wb_active_on_cloud_count AS wb_active_on_cloud_count,
       neflix_count.wb_hh_netflix_launch_count AS wb_hh_netflix_launch_count,
       neflix_count.wb_total_netflix_launch_count AS wb_total_netflix_launch_count,
       vod_usage.vod_active_accts AS wb_active_on_vod,
       vod_usage.total_number_of_views AS total_number_of_views,
       vod_usage.views_per_hh AS views_per_hh,
       vod_usage.total_view_duration_in_hours AS total_view_duration_in_hours,
       vod_usage.hours_per_hh AS hours_per_hh,
       vod_usage.free_view_duration_in_hours AS free_view_duration_in_hours,
       vod_usage.transactional_view_duration_in_hours AS transactional_view_duration_in_hours,
       vod_usage.premium_view_duration_in_hours AS premium_view_duration_in_hours,
       vod_usage.free_views AS free_views,
       vod_usage.transactional_views AS transactional_views,
       vod_usage.premium_views AS premium_views
FROM  
        (-------
        -- 2. WB Deployed Count on Newtown Controller
        -------
          SELECT run_date,
                 COUNT(DISTINCT account__number_aes256) wb_deployed_count
            FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
           WHERE run_date >= '2018-02-12'
             AND equipment__model RLIKE '^SP'     
             AND account__type = 'SUBSCRIBER'
             AND customer__type = 'Residential'
             AND system__controller_name = 'NEWTOWN'
             AND account__number_aes256 NOT IN  ('CQgl7Bf4I3HiVMt3ADHsDCWVBt5umlcZMb8qqzNbvcw=',
                                                 'FcIGT0rW/nxyVV8D/hsDMKzABDbcXjZrIZ3xRbOdkEs=',
                                                 'ZUnZ49j6tS4bt5EaxFcaWAP1lGfKdyESpqiZpfNzI/E=',
                                                 'fXn13Tz9B8ot6EzwqnpAQOjpVWuqeIvTYaKBWBE2Xak=',
                                                 'wAsUbC2d8Gp4oouQowPBlSqyNJsQEoI3p/aIqkYtAFQ=') -- exclude currently known account__dwelling_description = 'Residential Test Account' accounts until ETL is updated
        GROUP BY run_date) AS deployed_count
JOIN (
        -------
        -- 3. WB HH Active On Cloud
        -------
        SELECT partition_date_denver,
               COUNT(DISTINCT account__number_aes256) wb_active_on_cloud_count
          FROM dev.brian_wb_sg_activity
          WHERE visit__application_details__application_name = 'Spectrum Guide'
          GROUP BY partition_date_denver) as active_count
JOIN (         
        -------
        -- 4. WB HH with NetFlix Launches, Total Launches
        -------
        SELECT partition_date_denver,
               COUNT(DISTINCT CASE WHEN state__content__identifiers__tms_guide_id = 'SH025232490000' THEN account__number_aes256 ELSE NULL END) AS wb_hh_netflix_launch_count,
               COUNT(CASE WHEN state__content__identifiers__tms_guide_id = 'SH025232490000' THEN account__number_aes256 ELSE NULL END) AS wb_total_netflix_launch_count
               FROM dev.brian_wb_sg_activity
               --WHERE state__content__identifiers__tms_guide_id = 'SH025232490000'
               GROUP BY partition_date_denver) AS neflix_count
JOIN (
        -------
        -- 5. WB HH VOD Usage
        -------
        SELECT wb.run_date, 
               COUNT(DISTINCT wb.account__number_aes256) AS wb_count_accts,
               COUNT(DISTINCT a.account__number_aes256) AS vod_active_accts,
               ROUND(SUM(a.free_view_duration_in_s)/3600,2) AS free_view_duration_in_hours,
               ROUND(SUM(a.trans_view_duration_in_s)/3600,2) AS transactional_view_duration_in_hours,
               ROUND(SUM(a.prem_view_duration_in_s)/3600,2) AS premium_view_duration_in_hours,
               ROUND(SUM(a.total_view_duration_in_s)/3600,2) AS total_view_duration_in_hours,
               ROUND((SUM(a.total_view_duration_in_s)/3600)/COUNT(DISTINCT a.account__number_aes256),2) AS hours_per_hh,
               SUM(a.free_views) AS free_views,
               SUM(a.trans_views) AS transactional_views,
               SUM(a.prem_views) AS premium_views,
               SUM(a.total_number_of_views) AS total_number_of_views,
               ROUND(SUM(a.total_number_of_views)/COUNT(DISTINCT a.account__number_aes256),2) AS views_per_hh
        FROM prod.vod_acct_level_usage_daily_agg AS a
        RIGHT JOIN (  SELECT run_date,
                             account__number_aes256
                        FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
                       WHERE run_date >= '2018-02-12'
                         AND equipment__model RLIKE '^SP'     
                         AND account__type = 'SUBSCRIBER'
                         AND customer__type = 'Residential'
                         AND system__controller_name = 'NEWTOWN'
                         AND account__number_aes256 NOT IN  ('CQgl7Bf4I3HiVMt3ADHsDCWVBt5umlcZMb8qqzNbvcw=',
                                                             'FcIGT0rW/nxyVV8D/hsDMKzABDbcXjZrIZ3xRbOdkEs=',
                                                             'ZUnZ49j6tS4bt5EaxFcaWAP1lGfKdyESpqiZpfNzI/E=',
                                                             'fXn13Tz9B8ot6EzwqnpAQOjpVWuqeIvTYaKBWBE2Xak=',
                                                             'wAsUbC2d8Gp4oouQowPBlSqyNJsQEoI3p/aIqkYtAFQ=') -- exclude currently known account__dwelling_description = 'Residential Test Account' accounts until account ETL is updated
                    ) AS wb
        ON a.account__number_aes256 = wb.account__number_aes256
        AND a.partition_date_time = wb.run_date 
        and a.total_view_duration_in_s > 0
        GROUP BY run_date) AS vod_usage
    ON deployed_count.run_date = active_count.partition_date_denver
    AND deployed_count.run_date = neflix_count.partition_date_denver
    AND deployed_count.run_date = vod_usage.run_date
    ORDER BY run_date;