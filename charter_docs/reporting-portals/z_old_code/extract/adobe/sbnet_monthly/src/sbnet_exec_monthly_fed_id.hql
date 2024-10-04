USE ${env:ENVIRONMENT};

TRUNCATE TABLE ${env:TMP_db}.sbnet_fed_ID_auths_monthly;

-- Auth Successes broken out by legacy company
INSERT INTO TABLE ${env:TMP_db}.sbnet_fed_ID_auths_monthly
SELECT 
    date_yearmonth('${hiveconf:MONTH_START_DATE}') AS year_month,
    source_app AS source_app,
    CAST(NULL AS INT) AS total_login_attempts,
    SUM(IF(check_level='Success',total,0)) AS total_login_successes,
    CAST(NULL AS INT) AS percent_login_success,
    CAST(NULL AS INT) AS unique_users_logged_in,
    concat('L-',IF(footprint='Charter','CHTR',footprint)) AS company
FROM
    federated_id_auth_attempts_total
WHERE 
    (partition_date_hour_denver LIKE '${env:YEAR_MONTH_WC}')
    AND source_app = 'portals-idp-comm'
GROUP BY
    date_yearmonth('${hiveconf:MONTH_START_DATE}'),
    source_app,
    concat('L-',IF(footprint='Charter','CHTR',footprint))
;

-- Auth attempts combined all companies
INSERT INTO TABLE ${env:TMP_db}.sbnet_fed_ID_auths_monthly
SELECT 
    date_yearmonth('${hiveconf:MONTH_START_DATE}') AS year_month,
    source_app AS source_app,
    SUM(IF(check_level='Success' OR check_level='Failure',total,0)) AS total_login_attempts,
    CAST(NULL AS INT) AS total_login_successes,
    CAST(NULL AS INT) AS percent_login_success,
    CAST(NULL AS INT) AS unique_users_logged_in,
    'L-CHTR' AS company -- using L-CHTR as alias even though it represents combine companies, makes JOIN much easier later
FROM
    federated_id_auth_attempts_total
WHERE 
    (partition_date_hour_denver LIKE '${env:YEAR_MONTH_WC}')
    AND source_app = 'portals-idp-comm'
GROUP BY
    date_yearmonth('${hiveconf:MONTH_START_DATE}'),
    source_app
;

-- Unique Users Logged in Broken Out by Legacy Company
INSERT INTO TABLE ${env:TMP_db}.sbnet_fed_ID_auths_monthly
SELECT
    date_yearmonth('${hiveconf:MONTH_START_DATE}') as year_month,
    source_app,
    CAST(NULL AS INT) AS total_login_attempts,
    CAST(NULL AS INT) AS total_login_successes,
    CAST(NULL AS INT) AS percent_login_success,
    SIZE(collect_set(username)) as unique_users_logged_in,
    CASE
        WHEN charter_login <> '' THEN 'L-CHTR'
        WHEN twc_login <> '' THEN 'L-TWC'
        WHEN bh_login <> '' THEN 'L-BHN'
        ELSE NULL
    END AS company
FROM federated_id
WHERE 
    (partition_date_hour_denver LIKE '${env:YEAR_MONTH_WC}')
    AND source_app IN ('portals-idp-comm') -- Business
    AND response_code = 'UNIQUE_AUTH'
GROUP BY 
    date_yearmonth('${hiveconf:MONTH_START_DATE}'),
    source_app,
    CASE
        WHEN charter_login <> '' THEN 'L-CHTR'
        WHEN twc_login <> '' THEN 'L-TWC'
        WHEN bh_login <> '' THEN 'L-BHN'
        ELSE NULL
    END
;