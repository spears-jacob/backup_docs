-------------------------------------------------------------------------------

--Populates the temp table new_id_counts_all with new id counts by company
--Populates data from various sources for L-CHTR, L-BHN, and L-TWC

-------------------------------------------------------------------------------

USE ${env:ENVIRONMENT};

--------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-CHTR New ID Calculations --

INSERT INTO ${env:TMP_db}.new_id_counts_all
SELECT
SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.bam' THEN visit__visit_id ELSE NULL END)) + SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.btm' THEN visit__visit_id ELSE NULL END)) + SIZE(COLLECT_SET(case when message__name = 'my-account.create-id-final.nbtm' THEN visit__visit_id ELSE NULL END)) AS new_ids_charter_count_all,
'${env:YEAR_MONTH}' AS year_month,
'L-CHTR' AS company
FROM
net_events
WHERE
    partition_date BETWEEN '${env:MONTH_START_DATE}' 
    AND '${env:MONTH_END_DATE}'
GROUP BY
date_yearmonth(partition_date)
;

SELECT '*****-- End L-CHTR New ID Calculations --*****' -- 95.406 seconds
;


-- End L-CHTR New ID Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-BHN New ID Calculations --

INSERT INTO ${env:TMP_db}.new_id_counts_all
SELECT
MAX(IF(site_id='003' and role='Administrator',CAST(REGEXP_REPLACE(REGEXP_REPLACE(new_accounts,'"',''),',','') AS BIGINT),NULL)) AS new_ids_charter_count_all,
'${env:YEAR_MONTH}' AS year_month,
'L-BHN' AS company
FROM
${env:TMP_db}.sbnet_exec_monthly_bhn_accounts_manual
WHERE year_month = '${env:YEAR_MONTH}'
GROUP BY
year_month
;

SELECT '*****-- End L-BHN New ID Calculations --*****' -- 95.406 seconds
;

-- End L-BHN New ID Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- Begin L-TWC New ID Calculations --

INSERT INTO ${env:TMP_db}.new_id_counts_all
SELECT
SUM(IF(array_contains(message__feature__name,'Custom Event 5'),1,0)) AS new_ids_charter_count_all,
'${env:YEAR_MONTH}' AS year_month,
'L-TWC' AS company
FROM
twc_residential_global_events
WHERE
    (partition_date_utc BETWEEN DATE_ADD('${env:MONTH_START_DATE}',-1) AND DATE_ADD('${env:MONTH_END_DATE}',1))
    AND epoch_converter(CAST(message__timestamp*1000 AS BIGINT),'America/Denver') BETWEEN '${env:MONTH_START_DATE}' AND '${env:MONTH_END_DATE}'
;

SELECT '*****-- End L-TWC New ID Calculations --*****' -- 95.406 seconds
;

-- End L-TWC New ID Calculations --
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-- End New ID Calculations --

SELECT '*****-- End New ID Calculations --*****' -- 95.406 seconds
;
