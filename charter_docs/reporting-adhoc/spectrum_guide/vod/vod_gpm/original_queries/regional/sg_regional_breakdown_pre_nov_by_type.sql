SELECT 'legacy' AS spectrum_or_legacy, 
        category,
        region,
        vod.month_of,
        SIZE(COLLECT_SET(deployed.equipment__derived_mac_address_aes256)) AS stb_count,
        SIZE(COLLECT_SET(deployed.account__number_aes256)) AS hh_count,
        SUM(session__viewing_in_s) AS viewing_in_s,
        COUNT(*) AS total_views

FROM 


(         SELECT LAST_DAY(partition_date_time) AS month_of,
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
                session__viewing_in_s
         FROM prod.vod_concurrent_stream_session_event
         WHERE partition_date_time BETWEEN '2016-08-01' AND '2016-10-31'
           AND session__viewing_in_s < 14400
           AND session__viewing_in_s > 0 
           AND session__is_error = FALSE

 ) vod

JOIN 
( SELECT * FROM test.monthly_deployed_sg
WHERE sg_deployed_type = 'NEW_CONNECT'
) deployed

ON deployed.equipment__derived_mac_address_aes256 = vod.account__mac_id_aes256
AND deployed.month_of = vod.month_of
GROUP BY 'legacy', vod.month_of, category, region
