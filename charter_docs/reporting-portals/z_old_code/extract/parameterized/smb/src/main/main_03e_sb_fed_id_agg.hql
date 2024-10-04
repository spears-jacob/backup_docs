
-------------------------------------------------------------------------------
-- Drop and rebuild sb_fed_ID_ temp table --

DROP TABLE IF EXISTS ${env:TMP_db}.sb_fed_ID_${env:CADENCE} PURGE
;

CREATE TEMPORARY TABLE IF NOT EXISTS ${env:TMP_db}.sb_fed_ID_${env:CADENCE}
(
  ${env:pf} STRING,
  source_app STRING,
  total_login_attempts bigint,
  total_login_successes bigint,
  hhs_logged_in bigint,
  company STRING
)
;

SELECT '*****-- END - Drop and rebuild sb_fed_ID_monthly temp table --*****'
;

-- END - Drop and rebuild sbnet_fed_ID_cadence temp table --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-- Begin L-CHTR Federated ID Calculations SPLUNK (before 6/2018) --

INSERT INTO TABLE ${env:TMP_db}.sb_fed_ID_${env:CADENCE}

SELECT  ${env:aplh} as ${env:pf},
        source_app AS source_app,
        COUNT(*) AS total_login_attempts,
        SUM(IF(response_code = 'UNIQUE_AUTH', 1, 0)) AS total_login_successes,
        SIZE(COLLECT_SET(IF(response_code = 'UNIQUE_AUTH', username, NULL))) AS hhs_logged_in,
        CASE
            WHEN charter_login <> '' THEN 'L-CHTR'
            WHEN twc_login <> '' THEN 'L-TWC'
            WHEN bh_login <> '' THEN 'L-BHN'
            ELSE 'UNAUTH'
        END AS company
FROM asp_v_federated_id
LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm
     on SUBSTRING(partition_date_hour_denver,0,10) = partition_date
WHERE (    partition_date_hour_denver < '2018-06-01_00'
       AND partition_date_hour_denver >=  "${env:START_DATE_HOUR}"
       AND partition_date_hour_denver <  "${env:END_DATE_HOUR}"   )
    AND source_app = 'portals-idp-comm'  -- smb
GROUP BY  ${env:aplh},
          source_app,
          CASE
              WHEN charter_login <> '' THEN 'L-CHTR'
              WHEN twc_login <> '' THEN 'L-TWC'
              WHEN bh_login <> '' THEN 'L-BHN'
              ELSE 'UNAUTH'
          END
  ;


  -------------------
  -- Begin L-CHTR Federated ID Calculations KINESIS (after 6/2018) --


  INSERT INTO TABLE ${env:TMP_db}.sb_fed_ID_${env:CADENCE}

  SELECT  ${env:aplh} as ${env:pf},
          source_app AS source_app,
          COUNT(trace_id) AS total_login_attempts,
          COUNT(if(is_success = true,trace_id,null)) as total_successes,
          SIZE(COLLECT_SET(IF(account_number_aes256 IS NOT NULL,account_number_aes256, NULL))) AS hhs_logged_in,
          CASE
              WHEN footprint = 'Charter' THEN 'L-CHTR'
              WHEN footprint = 'TWC' THEN 'L-TWC'
              WHEN footprint = 'BHN' THEN 'L-BHN'
              ELSE 'UNAUTH'
          END AS company
  FROM asp_v_federated_identity
  LEFT JOIN ${env:LKP_db}.${env:fm_lkp} fm
       on epoch_converter(cast(event_timestamp as bigint),'America/Denver') = partition_date
  WHERE ( epoch_converter(cast(event_timestamp as bigint),'America/Denver') >= '2018-06-01'
          AND epoch_datehour(cast(event_timestamp as bigint),'America/Denver') >=  "${env:START_DATE_HOUR}"
          AND epoch_datehour(cast(event_timestamp as bigint),'America/Denver') <  "${env:END_DATE_HOUR}"   )
          AND LOWER(source_app) IN('portals-idp-comm')
          AND account_number_aes256 IS NOT NULL
          AND is_success = true
  GROUP BY  ${env:aplh},
            source_app,
            footprint
  ;



  -------------------------------------------------------------------------------
  -- END Federated ID Calculations --

  SELECT '*****-- END Federated ID Calculations --*****'
  ;

  SELECT '\n\n*****-- Now Inserting Fed ID Calculations into asp_${env:CADENCE}_agg_raw --*****\n\n'
  ;

  INSERT OVERWRITE TABLE asp_${env:CADENCE}_agg_raw PARTITION(domain, company, ${env:pf}, metric)

  SELECT value, unit, domain, company, ${env:pf}, metric
  FROM  (SELECT  'total_login_attempts' as metric,
                 total_login_attempts AS value,
                 'Visits' as unit,
                 'sb' as domain,
                 company,
                 ${env:pf}
         FROM ${env:TMP_db}.sb_fed_ID_${env:CADENCE}
         UNION
         SELECT  'total_login_successes' as metric,
                 total_login_successes AS value,
                 'Visits' as unit,
                 'sb' as domain,
                 company,
                 ${env:pf}
        FROM ${env:TMP_db}.sb_fed_ID_${env:CADENCE}
        WHERE total_login_successes <> 0
        UNION
        SELECT  'hhs_logged_in' as metric,
                hhs_logged_in AS value,
                'Visits' as unit,
                'sb' as domain,
                company,
                ${env:pf}
        FROM ${env:TMP_db}.sb_fed_ID_${env:CADENCE}
        WHERE hhs_logged_in <> 0
      ) q
  WHERE company <> 'UNAUTH'
  ;
