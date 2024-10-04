INSERT INTO TABLE test.vod_views_monthly_sg_region
SELECT 'sg' AS spectrum_or_legacy, 
        region,
        vod.month_of,
        SIZE(COLLECT_SET(deployed.account__mac_id_aes256)) AS stb_count,
        SIZE(COLLECT_SET(deployed.account__number_aes256)) AS hh_count,
        SUM(session__viewing_in_s) AS viewing_in_s,
        COUNT(*) AS total_views

FROM 


(    SELECT LAST_DAY(partition_date_time) as month_of,
       session__vod_lease_sid,
       session__equipment_mac_id_aes256,
       title__service_category_name,
       SUM(session__viewing_in_s) AS session__viewing_in_s
FROM prod.vod_concurrent_stream_session_event
WHERE partition_date_time BETWEEN '2017-01-01' AND '2017-02-20'
           AND title__service_category_name IN ('FHD',
                                                'FOD',
                                                'AOD',
                                                'EHD',
                                                'MOD',
                                                'MHD',
                                                'EOD',
                                                'AHD',
                                                'SVOD',
                                                'POD',
                                                'PHD')
           AND session__viewing_in_s < 14400
         AND session__viewing_in_s > 0
        AND session__vod_lease_sid IS NOT NULL
           AND session__is_error = FALSE
         GROUP BY partition_date_time,
                  session__vod_lease_sid,
                  session__equipment_mac_id_aes256,
                  title__service_category_name
 ) vod

JOIN 
( SELECT * FROM test.monthly_deployed_sg
WHERE  sg_deployed_type = 'NEW_CONNECT'
) deployed

ON deployed.account__mac_id_aes256 = vod.session__equipment_mac_id_aes256
AND deployed.month_of = vod.month_of
GROUP BY 'sg', vod.month_of, region
