-------------------------------------------------------------------------------

--Populates the temp table net_fed_ID_monthly with aggregated Federated ID data
--Populates data from various sources for L-CHTR, L-BHN, L-TWC, and Total Combined

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

-------------------------------------------------------------------------------
-- Begin L-CHTR Federated ID Calculations --

INSERT INTO TABLE ${env:TMP_db}.net_fed_ID_monthly
SELECT
'${env:YEAR_MONTH}' AS year_month,
fid1.source_app AS source_app,
fid3.total_login_attempts AS total_login_attempts, -- Grouping under 'L-CHTR' for join even though this value represents all L-CHTR, L-BHN, & L-TWC
SUM(IF(fid1.check_level='Success',total,0)) AS total_login_successes,
CAST(NULL AS INT) AS percent_login_success,
fid2.hhs_logged_in AS hhs_logged_in,
CONCAT('L-',IF(fid1.footprint='Charter','CHTR',fid1.footprint)) AS company
FROM
federated_id_auth_attempts_total fid1
--
LEFT JOIN
    (
    SELECT
    '${env:YEAR_MONTH}' as year_month,
    fidx.source_app as sourceapp,
    SIZE(COLLECT_SET(fidx.username)) as hhs_logged_in,
    CASE
    WHEN fidx.charter_login <> '' THEN 'L-CHTR'
    WHEN fidx.twc_login <> '' THEN 'L-TWC'
    WHEN fidx.bh_login <> '' THEN 'L-BHN'
    ELSE NULL
    END AS company
    FROM federated_id fidx
    WHERE
    fidx.partition_date_hour_denver LIKE '${env:YEAR_MONTH_WC}'
    AND fidx.source_app IN ('portals-idp') -- Resi
    AND fidx.response_code = 'UNIQUE_AUTH'
    GROUP BY
    '${env:YEAR_MONTH}',
    fidx.source_app,
    CASE
    WHEN fidx.charter_login <> '' THEN 'L-CHTR'
    WHEN fidx.twc_login <> '' THEN 'L-TWC'
    WHEN fidx.bh_login <> '' THEN 'L-BHN'
    ELSE NULL
    END
    ) fid2
ON CONCAT('L-',IF(fid1.footprint='Charter','CHTR',fid1.footprint)) = fid2.company
AND '${env:YEAR_MONTH}' = fid2.year_month
--
LEFT JOIN
    (SELECT
    '${env:YEAR_MONTH}' AS year_month,
    CAST(NULL AS INT) AS source_app,
    SUM(IF(fidz.check_level='Success' OR fidz.check_level='Failure',total,0)) AS total_login_attempts,
    CAST(NULL AS INT) AS total_login_successes,
    CAST(NULL AS INT) AS percent_login_success,
    CAST(NULL AS INT) AS hhs_logged_in,
    'L-CHTR' AS company
    FROM
    federated_id_auth_attempts_total fidz
    WHERE
    partition_date_hour_denver LIKE '${env:YEAR_MONTH_WC}'
    AND fidz.source_app = 'portals-idp'
    AND fidz.footprint IN ('Charter','BHN','Charter/BHN','Charter/Charter','Charter/TWC/BHN','TWC/BHN','TWC')    
    ) fid3
ON fid3.year_month = '${env:YEAR_MONTH}'
AND fid3.company= 'L-CHTR'
--
WHERE
fid1.partition_date_hour_denver LIKE '${env:YEAR_MONTH_WC}'
AND fid1.source_app = 'portals-idp'
AND fid1.footprint IN('Charter')
GROUP BY
fid1.source_app,
concat('L-',IF(fid1.footprint='Charter','CHTR',fid1.footprint)),
fid2.hhs_logged_in,
fid3.total_login_attempts
;

SELECT '*****-- End L-CHTR Federated ID Calculations --*****' -- 14.842 seconds
;

-- End L-CHTR Federated ID Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-BHN Federated ID Calculations --


INSERT INTO TABLE ${env:TMP_db}.net_fed_ID_monthly
SELECT
'${env:YEAR_MONTH}' AS year_month,
fid1.source_app AS source_app,
CAST(NULL AS INT) AS total_login_attempts,
SUM(IF(fid1.check_level='Success',fid1.total,0)) AS total_login_successes,
CAST(NULL AS INT) AS percent_login_success,
fid2.hhs_logged_in AS hhs_logged_in,
CONCAT('L-',IF(fid1.footprint='Charter','CHTR',fid1.footprint)) AS company
FROM
federated_id_auth_attempts_total fid1
LEFT JOIN
    (
    SELECT
    '${env:YEAR_MONTH}' as year_month,
    fidx.source_app as sourceapp,
    SIZE(COLLECT_SET(fidx.username)) as hhs_logged_in,
    CASE
    WHEN fidx.charter_login <> '' THEN 'L-CHTR'
    WHEN fidx.twc_login <> '' THEN 'L-TWC'
    WHEN fidx.bh_login <> '' THEN 'L-BHN'
    ELSE NULL
    END AS company
    FROM federated_id fidx
    WHERE
    fidx.partition_date_hour_denver LIKE '${env:YEAR_MONTH_WC}'
    AND fidx.source_app IN ('portals-idp')
    AND fidx.response_code = 'UNIQUE_AUTH'
    GROUP BY
    '${env:YEAR_MONTH}',
    fidx.source_app,
    CASE
    WHEN fidx.charter_login <> '' THEN 'L-CHTR'
    WHEN fidx.twc_login <> '' THEN 'L-TWC'
    WHEN fidx.bh_login <> '' THEN 'L-BHN'
    ELSE NULL
    END
    ) fid2
ON CONCAT('L-',IF(fid1.footprint='Charter','CHTR',fid1.footprint)) = fid2.company
AND '${env:YEAR_MONTH}' = fid2.year_month
WHERE
fid1.partition_date_hour_denver LIKE '${env:YEAR_MONTH_WC}'
AND fid1.source_app = 'portals-idp'
AND fid1.footprint IN('BHN')
GROUP BY
fid1.source_app,
concat('L-',IF(fid1.footprint='Charter','CHTR',fid1.footprint)),
fid2.hhs_logged_in
;

SELECT '*****-- End L-BHN Federated ID Calculations --*****' --
;

-- End L-BHN Federated ID Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-TWC Federated ID Calculations --

INSERT INTO TABLE ${env:TMP_db}.net_fed_ID_monthly
SELECT
'${env:YEAR_MONTH}' AS year_month,
fid1.source_app AS source_app,
CAST(NULL AS INT) AS total_login_attempts,
SUM(IF(fid1.check_level='Success',fid1.total,0)) AS total_login_successes,
CAST(NULL AS INT) AS percent_login_success,
fid2.hhs_logged_in AS hhs_logged_in,
CONCAT('L-',IF(fid1.footprint='Charter','CHTR',fid1.footprint)) AS company
FROM
federated_id_auth_attempts_total fid1
LEFT JOIN
    (
    SELECT
    '${env:YEAR_MONTH}' as year_month,
    fidx.source_app as sourceapp,
    SIZE(COLLECT_SET(fidx.username)) as hhs_logged_in,
    CASE
    WHEN fidx.charter_login <> '' THEN 'L-CHTR'
    WHEN fidx.twc_login <> '' THEN 'L-TWC'
    WHEN fidx.bh_login <> '' THEN 'L-BHN'
    ELSE NULL
    END AS company
    FROM federated_id fidx
    WHERE
    fidx.partition_date_hour_denver LIKE '${env:YEAR_MONTH_WC}'
    AND fidx.source_app IN ('portals-idp')
    AND fidx.response_code = 'UNIQUE_AUTH'
    GROUP BY
    '${env:YEAR_MONTH}',
    fidx.source_app,
    CASE
    WHEN fidx.charter_login <> '' THEN 'L-CHTR'
    WHEN fidx.twc_login <> '' THEN 'L-TWC'
    WHEN fidx.bh_login <> '' THEN 'L-BHN'
    ELSE NULL
    END
    ) fid2
ON CONCAT('L-',IF(fid1.footprint='Charter','CHTR',fid1.footprint)) = fid2.company
AND '${env:YEAR_MONTH}' = fid2.year_month
WHERE
    fid1.partition_date_hour_denver LIKE '${env:YEAR_MONTH_WC}'
    AND fid1.source_app = 'portals-idp'
    AND fid1.footprint IN('TWC')
GROUP BY
fid1.source_app,
concat('L-',IF(fid1.footprint='Charter','CHTR',fid1.footprint)),
fid2.hhs_logged_in
;

SELECT '*****-- End L-TWC Federated ID Calculations --*****' --
;

-- End L-TWC Federated ID Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- END Federated ID Calculations --

SELECT '*****-- END Federated ID Calculations --*****'
;
