DROP TABLE TEST_TMP.biglittle_vod;
CREATE TABLE TEST_TMP.biglittle_vod AS

SELECT partition_date_time,
        session__equipment_mac_id_aes256,
        series
FROM
(
SELECT partition_date_time,
        session__equipment_mac_id_aes256,
       CASE WHEN lower(title__name) LIKE 'big little lies%' OR lower(title__name) LIKE 'big ltl lies 0%'
                THEN 'Big Little Lies'
        ELSE 'other' END AS series
FROM prod.vod_concurrent_stream_session_event
WHERE asset__class_code = 'MOVIE'
AND session__is_error = FALSE
AND title__service_category_name != 'ADS'
AND partition_date_time BETWEEN '2017-01-01' AND '2017-04-30'
) d
WHERE series != 'other'
GROUP BY partition_date_time,
        session__equipment_mac_id_aes256,
        series;

----
DROP TABLE IF EXISTS TEST_TMP.vod_biglittle_equip;
CREATE TABLE TEST_TMP.vod_biglittle_equip AS
SELECT asset.partition_date_time,
       asset.account__number_aes256,
       series
FROM
(

SELECT  asset.partition_date_time,
        session__equipment_mac_id_aes256,
        account__number_aes256,
        series

FROM TEST_TMP.biglittle_vod asset

JOIN

(SELECT account__number_aes256,
        equipment__derived_mac_address_aes256,
        partition_date_time
FROM prod.account_equipment_history
WHERE partition_date_time BETWEEN '2017-01-01' AND LAST_DAY('2017-04-01') ) equip
ON session__equipment_mac_id_aes256 = equipment__derived_mac_address_aes256
AND equip.partition_date_time = asset.partition_date_time ) asset

JOIN

(SELECT account__number_aes256,
        partition_date_time
 FROM prod.account_history
 WHERE partition_date_time BETWEEN '2017-01-01' AND LAST_DAY('2017-04-01')
 AND customer__type = 'Residential'
 AND account__type = 'SUBSCRIBER'
 AND product__is_video_package = TRUE
 ) account

 ON asset.account__number_aes256 = account.account__number_aes256
 AND asset.partition_date_time = account.partition_date_time;
---
DROP TABLE IF EXISTS TEST.vod_biglittle_final;
CREATE TABLE TEST.vod_biglittle_final AS 
SELECT account__number_aes256,
        series
 FROM TEST_TMP.vod_biglittle_equip
GROUP BY account__number_aes256,
        series
