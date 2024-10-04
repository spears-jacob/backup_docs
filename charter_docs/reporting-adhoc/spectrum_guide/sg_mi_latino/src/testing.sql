

STEP 1 TEST:

SELECT product__is_spectrum_guide,
product__video_package_type,
SIZE(COLLECT_SET(account__number_aes256)),
SUM(stb_seconds_viewed)/3600,
(SUM(stb_seconds_viewed)/3600)/SIZE(COLLECT_SET(account__number_aes256))
FROM test.vod_sg_base
GROUP BY product__is_spectrum_guide,product__video_package_type

UNION ALL

SELECT product__is_spectrum_guide,
product__video_package_type,
SIZE(COLLECT_SET(account__number_aes256)),
SUM(stb_seconds_viewed)/3600,
(SUM(stb_seconds_viewed)/3600)/SIZE(COLLECT_SET(account__number_aes256))
FROM test_tmp.latino_vod_sg_base
GROUP BY product__is_spectrum_guide,product__video_package_type

;
