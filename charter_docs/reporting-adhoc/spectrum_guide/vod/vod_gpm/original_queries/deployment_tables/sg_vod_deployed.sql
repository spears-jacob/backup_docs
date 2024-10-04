
-- Table creation query.  month_of will be the value of the last day of the month.  For example if looking at March 2017, month_of = '2017-03-31'
CREATE IF NOT EXISTS  TABLE TEST.VOD_MONTHLY_SG_DEPLOYED (
  account__mac_id_aes256 STRING,
  account__number_aes256 STRING,
  region STRING,
  deployed_date STRING,
  account__category STRING,
  sg_deployed_type STRING )
  
  PARTITIONED BY ( month_of STRING );

-- INSERTS into deployed table for SG VOD monthly metrics.  macs are only used if they were subscribers for both the first and last day of the month

INSERT INTO TABLE TEST.VOD_MONTHLY_SG_DEPLOYED PARTITION (month_of)

SELECT account__mac_id_aes256,
  account__number_aes256,
  region,
  deployed_date,
  account__category,
  sg_deployed_type,
  month_of
FROM
(
SELECT month_of,
  account__mac_id_aes256,
  account__number_aes256,
  region,
  account__category,
  sg_deployed_type,
  deployed_date,
  COUNT(*) AS count_of

FROM
(
SELECT LAST_DAY(run_date) AS month_of,
    account__mac_id_aes256,
    account__number_aes256,
    sg_deployed_type,
    FROM_UNIXTIME(sg_account_deployed_timestamp, 'yyyy-MM-dd') AS deployed_date,
            CASE
              WHEN system__kma_desc IN ('Michigan (KMA)',
                                        'Michigan') THEN 'Michigan'
              WHEN system__kma_desc IN ('Central States (KMA)',
                                        'Central States') THEN 'Central States'
              WHEN system__kma_desc IN ('Mountain States (KMA)',
                                        'Mountain States') THEN 'Mountain States'
              WHEN system__kma_desc IN ('Minnesota/Nebraska (KMA)',
                                        'Minnesota') THEN 'Minnesota'
              WHEN system__kma_desc IN ('Northwest (KMA)',
                                        'Pacific Northwest',
                                        'Sierra Nevada') THEN 'Pacific Northwest/ Sierra Nevada'
              WHEN system__kma_desc IN ('TNLA/AL (KMA)',
                                        'TN / LA / AL') THEN 'TN / LA / AL'
              WHEN system__kma_desc IN ('Wisconsin  (KMA)',
                                        'Wisconsin') THEN 'Wisconsin'
              ELSE system__kma_desc
          END AS region,
    account__category
FROM prod_lkp.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY
  WHERE account__type='SUBSCRIBER'
AND run_date IN ('2017-03-01', LAST_DAY('2017-03-01'))
) deployed
GROUP BY month_of,
  account__mac_id_aes256,
  account__number_aes256,
  region,
  account__category,
  deployed_date,
  sg_deployed_type
) deploy
WHERE count_of = 2;

        
