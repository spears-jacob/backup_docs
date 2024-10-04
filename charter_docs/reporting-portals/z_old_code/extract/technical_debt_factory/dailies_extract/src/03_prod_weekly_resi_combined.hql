set hive.vectorized.execution.enabled = false;

USE ${env:ENVIRONMENT};

SELECT'
--------------------------------------------------------------------------------
-------------------------- ***** RESI COMBINED ***** ---------------------------
--------------------------------------------------------------------------------

';

SELECT'
--------------------------------------------------------------------------------
-------------------------- ***** Set Variables ***** ---------------------------
--------------------------------------------------------------------------------

';

SET domain=resi;
SET adobe=asp_metric_pivot_resi_adobe_wk;
SET quantum=asp_metric_pivot_resi_quantum_wk;
SET lkp=${env:LKP_db}.asp_pm_metric_lkp;
SET nulls=asp_pm_resi_adobe_np;
SET uniraw=asp_v_pm_resi_unified_raw;
SET adobe_stg=asp_prod_weekly_resi_adobe_stage;
SET quantum_stg=asp_prod_weekly_resi_quantum_stage;
SET stage=asp_prod_weekly_resi_stage;
SET format=asp_prod_weekly_resi;
SET tab=asp_v_prod_weekly_resi_tab;

SELECT'
--------------------------------------------------------------------------------
-------------------------- ***** Table Cleanup ***** ---------------------------
--------------------------------------------------------------------------------

';

TRUNCATE TABLE ${hiveconf:adobe_stg} PARTITION(platform,domain,company,data_source,week_end_dt);
TRUNCATE TABLE ${hiveconf:quantum_stg} PARTITION(platform,domain,company,data_source,week_end_dt);
TRUNCATE TABLE ${hiveconf:stage} PARTITION(platform,domain,company,data_source,week_end_dt);
DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:nulls} PURGE;

SELECT'
--------------------------------------------------------------------------------
-------------------- ***** STEP 1: NULL Placeholders ***** ---------------------
--------------------------------------------------------------------------------
';

DROP VIEW IF EXISTS ${hiveconf:uniraw};
CREATE VIEW IF NOT EXISTS ${hiveconf:uniraw}
AS
  SELECT
  *
  FROM ${hiveconf:adobe}

    UNION ALL

      SELECT
      *
      FROM ${hiveconf:quantum}
;

DROP TABLE IF EXISTS ${env:TMP_db}.${hiveconf:nulls};
CREATE TABLE IF NOT EXISTS ${env:TMP_db}.${hiveconf:nulls}
AS
SELECT DISTINCT
np1.week_end_dt,
np2.metric,
CAST(NULL AS STRING) AS metric_value,
np3.company,
np4.domain,
np5.platform
FROM ${hiveconf:uniraw} np1
  JOIN (
    SELECT DISTINCT
    metric
    FROM ${hiveconf:uniraw}
        ) np2
  JOIN (
    SELECT DISTINCT
    company
    FROM ${hiveconf:uniraw}
        ) np3
  JOIN (
    SELECT DISTINCT
    domain
    FROM ${hiveconf:uniraw}
        ) np4
  JOIN (
    SELECT DISTINCT
    platform
    FROM ${hiveconf:uniraw}
        ) np5
;

SELECT'
--------------------------------------------------------------------------------
---------------------- ***** STEP 2: Unify Metrics ***** -----------------------
--------------------------------------------------------------------------------
';

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** Adobe ***** -------------------------------
--------------------------------------------------------------------------------
';

INSERT INTO TABLE ${hiveconf:adobe_stg}
PARTITION(platform,domain,company,data_source,week_end_dt)
SELECT
lkp.metric_family,
src.metric_value,
np.metric,
lkp.report_suite,
lkp.company AS lkp_company,
lkp.portal,
lkp.hive_metric,
lkp.tableau_name,
lkp.quantum_start_fm,
lkp.associated_attempt_metric,
np.platform,
np.domain,
np.company,
'Adobe' AS data_source,
np.week_end_dt
FROM ${env:TMP_db}.${hiveconf:nulls} np
LEFT JOIN ${hiveconf:adobe} src
ON np.metric = src.metric
  AND np.company = src.company
  AND np.domain = src.domain
  AND np.week_end_dt = src.week_end_dt
LEFT JOIN ${hiveconf:lkp} lkp
  ON np.metric = lkp.hive_metric
    AND np.company = lkp.company
    AND lkp.report_suite = '${hiveconf:domain}'
;

SELECT'
--------------------------------------------------------------------------------
----------------------------- ***** Quantum ***** ------------------------------
--------------------------------------------------------------------------------
';

INSERT INTO TABLE ${hiveconf:quantum_stg}
PARTITION(platform,domain,company,data_source,week_end_dt)
SELECT
lkp.metric_family,
src.metric_value,
np.metric,
lkp.report_suite,
lkp.company AS lkp_company,
lkp.portal,
lkp.hive_metric,
lkp.tableau_name,
lkp.quantum_start_fm,
lkp.associated_attempt_metric,
np.platform,
np.domain,
np.company,
'Quantum' AS data_source,
np.week_end_dt
FROM ${env:TMP_db}.${hiveconf:nulls} np
LEFT JOIN ${hiveconf:quantum} src
ON np.metric = src.metric
  AND np.company = src.company
  AND np.domain = src.domain
  AND np.week_end_dt = src.week_end_dt
LEFT JOIN ${hiveconf:lkp} lkp
  ON np.metric = lkp.hive_metric
    AND np.company = lkp.company
    AND lkp.report_suite = '${hiveconf:domain}'
;


SELECT'
--------------------------------------------------------------------------------
---------------- ***** Filter "Activated" Quantum Metrics ***** ----------------
--------------------------------------------------------------------------------

';

INSERT INTO TABLE ${hiveconf:stage}
PARTITION(platform,domain,company,data_source,week_end_dt)
SELECT
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.metric_family
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.metric_family
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.metric_family
    ELSE sq.metric_family
  END AS metric_family,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.metric_value
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.metric_value
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.metric_value
    ELSE sq.metric_value
  END AS metric_value,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.metric
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.metric
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.metric
    ELSE sq.metric
  END AS metric,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.report_suite
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.report_suite
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.report_suite
    ELSE sq.report_suite
  END AS report_suite,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.lkp_company
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.lkp_company
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.lkp_company
    ELSE sq.portal
  END AS lkp_company,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.portal
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.portal
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.portal
    ELSE sq.portal
  END AS portal,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.hive_metric
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.hive_metric
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.hive_metric
    ELSE sq.hive_metric
  END AS hive_metric,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.tableau_name
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.tableau_name
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.tableau_name
    ELSE sq.tableau_name
  END AS tableau_name,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.quantum_start_fm
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.quantum_start_fm
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.quantum_start_fm
    ELSE sq.quantum_start_fm
  END AS quantum_start_fm,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.associated_attempt_metric
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.associated_attempt_metric
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.associated_attempt_metric
    ELSE sq.associated_attempt_metric
  END AS associated_attempt_metric,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.platform
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.platform
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.platform
    ELSE sq.platform
  END AS platform,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.domain
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.domain
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.domain
    ELSE sq.domain
  END AS domain,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.company
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.company
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.company
    ELSE sq.company
  END AS company,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.data_source
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.data_source
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.data_source
    ELSE sq.data_source
  END AS data_source,
CASE
    WHEN sq.quantum_start_fm IS NULL THEN sa.week_end_dt
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) < sq.quantum_start_fm THEN sa.week_end_dt
    WHEN sq.quantum_start_fm IS NOT NULL AND SUBSTRING(sq.week_end_dt,1,7) >= sq.quantum_start_fm THEN sq.week_end_dt
    ELSE sq.week_end_dt
  END AS week_end_dt
FROM ${hiveconf:quantum_stg} sq
LEFT JOIN ${hiveconf:adobe_stg} sa
  ON sq.metric = sa.hive_metric
    AND sq.company = sa.company
    AND sq.domain = sa.domain
    AND sq.report_suite = sa.report_suite
    AND sq.week_end_dt = sa.week_end_dt
;

SELECT'
--------------------------------------------------------------------------------
------------------------------ ***** Totals ***** ------------------------------
--------------------------------------------------------------------------------

';

INSERT OVERWRITE TABLE ${hiveconf:format}
PARTITION(platform,domain,company,data_source,week_end_dt)
SELECT
stg.metric_family,
stg.portal,
stg.hive_metric,
stg.tableau_name,
stg.tableau_name AS name_w_total,
stg.metric_value AS metric_value,
CAST(stg.metric_value AS STRING) AS metric_value_string,
tot.fm_metric_total,
CAST(tot.fm_metric_total AS STRING) AS fm_metric_total_string,
stg.quantum_start_fm,
stg.associated_attempt_metric,
stg.platform,
stg.domain,
stg.company,
stg.data_source,
stg.week_end_dt
FROM ${hiveconf:stage} stg
LEFT JOIN
(
  selecT
  week_end_dt,
  hive_metric,
  SUM(metric_value) AS fm_metric_total
  FROM ${hiveconf:stage}
  GROUP BY
  week_end_dt,
  hive_metric
) tot
  ON tot.week_end_dt = stg.week_end_dt
  AND tot.hive_metric = stg.hive_metric
;

INSERT OVERWRITE TABLE ${hiveconf:format}
PARTITION(platform,domain,company,data_source,week_end_dt)
SELECT
metric_family,
'-' AS portal,
hive_metric,
tableau_name,
CONCAT('Total - ',tableau_name) AS name_w_total,
SUM(metric_value) AS metric_value,
CAST(SUM(metric_value) AS STRING) AS metric_value_string,
fm_metric_total,
fm_metric_total_string,
'-' AS quantum_start_fm,
associated_attempt_metric,
platform,
domain,
'Total Combined' AS company,
'Totals' AS data_source,
week_end_dt
FROM ${hiveconf:format}
WHERE company <> 'Total Combined'
GROUP BY
metric_family,
'-',
hive_metric,
tableau_name,
fm_metric_total,
fm_metric_total_string,
'-',
CONCAT('Total - ',tableau_name),
platform,
domain,
'Total Combined',
'Totals',
week_end_dt,
associated_attempt_metric
;

SELECT'
--------------------------------------------------------------------------------
------------------------- ***** Excel Format ***** -----------------------------
--------------------------------------------------------------------------------


';

DROP VIEW IF EXISTS ${hiveconf:tab};
CREATE VIEW IF NOT EXISTS ${hiveconf:tab}
AS
SELECT DISTINCT
pm1.metric_family,
pm1.hive_metric,
pm1.tableau_name,
pm1.name_w_total,
pm1.company,
pm1.portal,
pm1.metric_value,
pm1.metric_value_string,
pm1.fm_metric_total,
pm1.fm_metric_total_string,
pm1.week_end_dt,
pm1.data_source,
pm1.platform,
pm1.domain,
pm1.quantum_start_fm,
pm1.associated_attempt_metric,
pm2.metric_value AS associated_attempt_metric_value,
CASE
  WHEN pm1.company = 'CHTR' THEN 1
  WHEN pm1.company = 'My Spectrum' THEN 2
  WHEN pm1.company = 'TWC' THEN 3
  WHEN pm1.company = 'MyTWC' THEN 4
  WHEN pm1.company = 'BHN' THEN 5
  WHEN pm1.company = 'Total Combined' THEN 7
  ELSE 6 END AS com_order
FROM ${hiveconf:format} pm1
LEFT JOIN ${hiveconf:format} pm2
  ON pm2.hive_metric = pm1.associated_attempt_metric
  AND pm2.company = pm1.company
  AND pm2.domain = pm1.domain
  AND pm2.week_end_dt = pm1.week_end_dt
ORDER BY
pm1.metric_family,
pm1.tableau_name,
pm1.week_end_dt,
com_order
;

SELECT'
--------------------------------------------------------------------------------
--------------- ***** END Resi Combined/Formatted Metrics ***** ----------------
--------------------------------------------------------------------------------

';
