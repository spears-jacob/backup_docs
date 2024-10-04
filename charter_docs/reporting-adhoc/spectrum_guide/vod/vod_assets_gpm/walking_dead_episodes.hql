INSERT INTO TABLE TEST_TMP.walking_dead_episodes

SELECT walking_dead.account__number_aes256,
        episode,
        walking_dead.session__viewing_in_s AS total_view_time_in_s
FROM
(
SELECT  d.partition_date_time,
        account__number_aes256,
        REGEXP_EXTRACT(title__name, '(Walking Dead [0-9]{3}).*',1) AS episode,
        session__viewing_in_s

FROM
(
SELECT  partition_date_time,
        session__equipment_mac_id_aes256,
        title__name,
        session__viewing_in_s
FROM prod.vod_concurrent_stream_session_event
WHERE asset__class_code = 'MOVIE'
        AND session__is_error = FALSE
        AND title__service_category_name != 'ADS'
        AND partition_date_time = '${run_date}'
        AND lower(title__name) LIKE 'walking dead 7%'
        AND session__viewing_in_s > 0
        AND session__viewing_in_s < 14400
) d
JOIN


(SELECT account__number_aes256,
        equipment__derived_mac_address_aes256,
        partition_date_time
FROM prod.account_equipment_history
WHERE partition_date_time = '${run_date}'
GROUP BY account__number_aes256,
        equipment__derived_mac_address_aes256,
        partition_date_time) equip
ON session__equipment_mac_id_aes256 = equipment__derived_mac_address_aes256
AND equip.partition_date_time = d.partition_date_time ) walking_dead

JOIN


(SELECT account__number_aes256,
        partition_date_time
 FROM prod.account_history
 WHERE partition_date_time = '${run_date}'
 AND customer__type = 'Residential'
 AND account__type = 'SUBSCRIBER'
 AND product__is_video_package = TRUE
  GROUP BY account__number_aes256,
        partition_date_time
 ) account

 ON walking_dead.account__number_aes256 = account.account__number_aes256
 AND walking_dead.partition_date_time = account.partition_date_time;
