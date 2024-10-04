SELECT account__number_aes256,
        series
 FROM TEST_TMP.vod_asset_equip 
GROUP BY account__number_aes256,
        series


