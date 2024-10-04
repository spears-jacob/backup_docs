INSERT INTO TABLE test.vod_dvr_rollup 

SELECT 'legacy' AS spectrum_or_legacy, 
        region,
        account__category,
        vod.month_of,
        SIZE(COLLECT_SET(deployed.equipment__derived_mac_address_aes256)) AS stb_count,
        SIZE(COLLECT_SET(deployed.account__number_aes256)) AS hh_count,
        SUM(session__viewing_in_s) AS viewing_in_s,
        COUNT(*) AS total_views
FROM
(
   SELECT LAST_DAY(partition_date_time) as month_of,
       session__vod_lease_sid,
       session__equipment_mac_id_aes256,
       title__service_category_name,
       SUM(session__viewing_in_s) AS session__viewing_in_s

FROM prod.vod_concurrent_stream_session_event
WHERE partition_date_time BETWEEN '2016-12-01' AND '2017-02-20'
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
( SELECT dep.*,
  dvr.account__category
FROM

( SELECT * FROM test.monthly_deployed_legacy2
 WHERE month_of >= '2016-12-01' ) dep
JOIN
--Used to determine if a household is DVR or NON_DVR.  
(SELECT * FROM test.legacy_dvr) dvr
ON dep.account__number_aes256 = dvr.account__number_aes256
AND dep.month_of = dvr.month_of ) deployed 

ON deployed.equipment__derived_mac_address_aes256 = vod.session__equipment_mac_id_aes256
AND deployed.month_of = vod.month_of
GROUP BY 'legacy', vod.month_of, region, account__category;
