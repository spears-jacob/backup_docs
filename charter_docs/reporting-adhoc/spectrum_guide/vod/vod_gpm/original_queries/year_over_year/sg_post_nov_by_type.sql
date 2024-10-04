INSERT INTO TABLE test.vod_views_monthly2 
SELECT 'sg' AS spectrum_or_legacy, 
        category,
        vod.month_of,
        SIZE(COLLECT_SET(deployed.account__mac_id_aes256)) AS stb_count,
        SIZE(COLLECT_SET(deployed.account__number_aes256)) AS hh_count,
        SUM(session__viewing_in_s) AS viewing_in_s,
        COUNT(*) AS total_views

FROM 


(         SELECT LAST_DAY(partition_date_time) AS month_of,
                session__vod_lease_sid,
                session__equipment_mac_id_aes256,
                          CASE
              WHEN title__service_category_name IN ('FHD',
                                                           'FOD') THEN 'Free'
              WHEN title__service_category_name IN ('AOD',
                                                    'EHD',
                                                    'MOD',
                                                    'MHD',
                                                    'EOD',
                                                    'AHD') THEN 'Transactional'
              WHEN title__service_category_name IN ('SVOD',
                                                    'POD',
                                                    'PHD') THEN 'Premium'
              ELSE 'Undefined'
          END AS category,
                SUM(session__viewing_in_s) AS session__viewing_in_s
         FROM prod.vod_concurrent_stream_session_event
         WHERE partition_date_time BETWEEN '2016-12-02' AND LAST_DAY('2016-12-01')
           AND session__viewing_in_s < 14400
           AND session__vod_lease_sid IS NOT NULL
         GROUP BY LAST_DAY(partition_date_time),
                  session__vod_lease_sid,
                  session__equipment_mac_id_aes256,
                CASE
              WHEN title__service_category_name IN ('FHD',
                                                           'FOD') THEN 'Free'
              WHEN title__service_category_name IN ('AOD',
                                                    'EHD',
                                                    'MOD',
                                                    'MHD',
                                                    'EOD',
                                                    'AHD') THEN 'Transactional'
              WHEN title__service_category_name IN ('SVOD',
                                                    'POD',
                                                    'PHD') THEN 'Premium'
              ELSE 'Undefined'
          END
 ) vod

JOIN 
( SELECT * FROM test.monthly_deployed_sg
WHERE month_of = LAST_DAY('2016-12-01')
) deployed

ON deployed.account__mac_id_aes256 = vod.session__equipment_mac_id_aes256
AND deployed.month_of = vod.month_of
GROUP BY 'sg', vod.month_of, category;
