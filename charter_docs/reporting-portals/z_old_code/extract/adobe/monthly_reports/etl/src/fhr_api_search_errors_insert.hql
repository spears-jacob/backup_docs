-- Monthly

-- data for month probably ready the day after the month ends
-- dependent on sg_p2_events and LKP.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY

-- Create table that holds history
-- Insert each monthly run

USE ${env:ENVIRONMENT};

INSERT INTO fhr_api_search_errors_monthly (year_month,api_search_error_count,average_search_response_time_in_ms)
SELECT 
  year_month AS year_month,
  SUM(if(application__api__response_code NOT IN
    ('200','201','202','203','204','205','206','207','208','209','210','226'), 1, 0)) AS api_search_error_count,
  AVG(application__api__response_time_in_ms) AS average_search_response_time_in_ms
FROM
(
  SELECT 
    spe.partition_date,
    spe.visit__device__enc_uuid,
    spe.application__api__response_code,
    spe.application__api__response_time_in_ms,
    SUBSTRING(spe.partition_date,0,7) AS year_month,
    mah.run_date,
    mah.account__mac_id_aes256
  FROM ${env:ENVIRONMENT}.sg_p2_events spe
  JOIN ${env:LKP_db}.SG_PHASEII_DEPLOYED_MACS_ALL_HISTORY mah
    ON mah.run_date = spe.partition_date
      AND spe.visit__device__enc_uuid = mah.account__mac_id_aes256
  WHERE 
  partition_date LIKE CONCAT(date_yearmonth(add_months("${env:TODAY_DASHED}",-1)),'%')
  AND  run_date LIKE CONCAT(date_yearmonth(add_months("${env:TODAY_DASHED}",-1)),'%')
  AND (application__api__response_code IS NOT NULL
  AND application__api__response_code != ''
  AND application__api__response_time_in_ms IS NOT NULL
  AND application__api__api_name = 'search')
  AND account__type ='SUBSCRIBER'
) deployed
GROUP BY year_month
;
