SELECT account__number_aes256,
        equipment__derived_mac_address_aes256,
        equipment__category_name,
        equipment__connect_date,
        region,
        month_of

FROM

(

SELECT equipment.account__number_aes256,
    equipment.equipment__derived_mac_address_aes256,
    equipment.equipment__category_name,
    equipment.equipment__connect_date,
    LAST_DAY(equipment.partition_date_time) AS month_of, 
    account.region,
    COUNT(*) AS count_of
FROM
( SELECT account__number_aes256,
        equipment__derived_mac_address_aes256,
        equipment__category_name,
        partition_date_time,
        equipment__connect_date
FROM prod.account_equipment_history
WHERE partition_date_time IN ( '2017-01-01', '2017-01-31' )
AND     equipment__category_name IN ('HD/DVR Converters',
        'Standard Digital Converters',
        'HD Converters') 
AND equipment__derived_mac_address_aes256 IS NOT NULL) equipment


JOIN

(SELECT account__number_aes256,
        partition_date_time,
 CASE WHEN system__kma_desc IN 
        ('Michigan (KMA)','Michigan') THEN 'Michigan'
      WHEN system__kma_desc IN 
        ('Northwest (KMA)', 'Pacific Northwest','Sierra Nevada' ) THEN 'Pacific Northwest/ Sierra Nevada'
      WHEN system__kma_desc IN 
        ('Central States (KMA)', 'Central States') THEN 'Central States'
      ELSE 'other'
      END as region
FROM prod.account_history
WHERE partition_date_time IN ('2017-01-01', '2017-01-31' )
AND     product__is_spectrum_guide = FALSE
AND system__kma_desc IN ('Michigan', 'Michigan (KMA)',
                                                'Central States (KMA)', 
                                                'Central States',
                                                'Pacific Northwest',
                                                'Sierra Nevada')
AND customer__type = 'Residential'
AND account__type = 'SUBSCRIBER' 
AND product__is_video_package = TRUE) account
 
ON account.account__number_aes256 = equipment.account__number_aes256
 AND account.partition_date_time = equipment.partition_date_time
 
GROUP BY equipment.account__number_aes256,
    equipment.equipment__derived_mac_address_aes256,
    equipment.equipment__category_name,
    equipment.equipment__connect_date,
    LAST_DAY(equipment.partition_date_time), 
    account.region
) deployed
WHERE count_of = 2;
