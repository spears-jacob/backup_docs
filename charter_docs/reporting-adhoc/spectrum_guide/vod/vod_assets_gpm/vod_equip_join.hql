--joining equipment history to accounts to get proper account information
CREATE TABLE TEST_TMP.vod_asset_equip AS
SELECT asset.partition_date_time,
       asset.account__number_aes256,
       series
FROM
(

SELECT  asset.partition_date_time,
        session__equipment_mac_id_aes256,
        account__number_aes256,
        series 
        
FROM TEST_TMP.vod_assets asset

JOIN 

(SELECT account__number_aes256,
        equipment__derived_mac_address_aes256,
        partition_date_time
FROM prod.account_equipment_history
WHERE partition_date_time BETWEEN '2017-01-01' AND LAST_DAY('2017-01-01') ) equip
ON session__equipment_mac_id_aes256 = equipment__derived_mac_address_aes256 
AND equip.partition_date_time = asset.partition_date_time ) asset

JOIN

(SELECT account__number_aes256,
        partition_date_time
 FROM prod.account_history
 WHERE partition_date_time BETWEEN '2017-01-01' AND LAST_DAY('2017-01-01') 
 AND customer__type = 'Residential'
 AND account__type = 'SUBSCRIBER' 
 AND product__is_video_package = TRUE
 ) account

 ON asset.account__number_aes256 = account.account__number_aes256
 AND asset.partition_date_time = account.partition_date_time

