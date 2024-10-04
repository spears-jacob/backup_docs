DROP TABLE TEST_TMP.amc_watch_times_monthly;
CREATE TABLE TEST_TMP.amc_watch_times_monthly AS

SELECT  amc.account__number_aes256,
        SUM(session__viewing_in_s) AS total_amc_viewing_in_s,
        SUM(CASE WHEN is_walking_dead = 'Walking Dead' THEN session__viewing_in_s ELSE 0 END) AS total_walking_dead_viewing_in_s
FROM
(SELECT equip.account__number_aes256,
        session__viewing_in_s,
        session__equipment_mac_id_aes256,
        CASE WHEN lower(title__name) LIKE 'walking dead 7%' Then 'Walking Dead'
        ELSE 'other' END AS is_walking_dead

FROM prod.vod_concurrent_stream_session_event vod

JOIN
(
SELECT equip.*
FROM
(SELECT account__number_aes256,
        equipment__derived_mac_address_aes256,
        partition_date_time
FROM prod.account_equipment_history
WHERE partition_date_time =  '2016-10-01'
GROUP BY account__number_aes256,
        equipment__derived_mac_address_aes256,
        partition_date_time
 ) equip
 JOIN
(SELECT account__number_aes256,
        partition_date_time
 FROM prod.account_history
 WHERE partition_date_time =  '2016-10-01'
 AND customer__type = 'Residential'
 AND account__type = 'SUBSCRIBER'
 AND product__is_video_package = TRUE
 GROUP BY account__number_aes256,
        partition_date_time
 ) account

 ON equip.account__number_aes256 = account.account__number_aes256
 AND equip.partition_date_time = account.partition_date_time
  ) equip

ON vod.session__equipment_mac_id_aes256 = equip.equipment__derived_mac_address_aes256
AND vod.partition_date_time = equip.partition_date_time
WHERE asset__class_code = 'MOVIE'
        AND session__is_error = FALSE
        AND title__service_category_name != 'ADS'
        AND vod.partition_date_time = '2016-10-01'
        AND lower(title__content_provider_name) LIKE 'amc%'
        AND session__viewing_in_s > 0
        AND session__viewing_in_s < 14400 ) amc

JOIN

TEST.walking_dead_accounts walking_dead
WHERE walking_dead.account__number_aes256 = amc.account__number_aes256
GROUP BY amc.account__number_aes256
