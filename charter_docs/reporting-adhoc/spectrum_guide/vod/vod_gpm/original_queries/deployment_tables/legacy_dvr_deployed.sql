CREATE TABLE test.legacy_dvr AS

SELECT month_of,
        account__number_aes256,
        stbs,
        CASE WHEN dvr_count >0 THEN 'DVR' ELSE 'NON_DVR' END AS account__category
FROM
(
SELECT month_of,
      SIZE(COLLECT_SET(CASE WHEN equipment__category_name = 'HD/DVR Converters'
           THEN equipment__derived_mac_address_aes256 END)) as dvr_count,
        COLLECT_SET(equipment__category_name) AS stbs,
        account__number_aes256
FROM test.monthly_deployed_legacy2
GROUP BY month_of,
    account__number_aes256 ) d;
