USE ${env:ENVIRONMENT};
set hive.vectorized.execution.enabled = false;

SELECT"

\n\n*****\n\nTable Cleanup\n\n*****\n\n

"
;

SELECT "

\n\n*****\n\nNow preparing temporary table for loading the display_layer\n\n*****\n\n

"
;

DROP TABLE IF EXISTS asp_${env:project_title}_display_layer
;

CREATE TABLE IF NOT EXISTS asp_${env:project_title}_display_layer
  (
    application   STRING,
    hive_metric   STRING,
    data_source   STRING,
    report        STRING,
    display_name  STRING,
    relevant_date STRING,
    notice_flag   STRING,
    company       STRING,
    portal        STRING,
    grain         STRING,
    unit          STRING,
    grouping_id   STRING,
    lookup_1      STRING,
    lookup_2      STRING,
    lookup_3      STRING,
    lookup_4      STRING,
    lookup_5      STRING,
    notes         STRING
  )
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='',"skip.header.line.count"="1")
;
--skips first line of .csv file containing headers

SELECT "

\n\n*****\n\nNow loading in ../bin/${env:project_title}/bin/display_layer.csv\n\n*****\n\n";

LOAD DATA LOCAL INPATH '../bin/${env:project_title}/bin/display_layer.csv' INTO TABLE asp_${env:project_title}_display_layer
;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

SELECT "

\n\n*****\n\nNow preparing table for loading the defintions\n\n*****\n\n

"
;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_${env:project_title}_metric_definitions_stage
;

CREATE TABLE ${env:TMP_db}.asp_${env:project_title}_metric_definitions_stage
  (
    run           STRING,
    hive_metric   STRING,
    application   STRING,
    version_note  STRING,
    note          STRING,
    start_date    STRING,
    end_date      STRING,
    condition_1   STRING,
    condition_2   STRING,
    condition_3   STRING,
    condition_4   STRING,
    condition_5   STRING,
    condition_6   STRING
  )
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='',"skip.header.line.count"="1")
;
--skips first line of .csv file containing headers

SELECT "

\n\n*****\n\nNow loading in ../bin/${env:project_title}/metric_repo/definitions.tsv\n\n*****\n\n

";

LOAD DATA LOCAL INPATH '../bin/${env:project_title}/metric_repo/definitions.tsv' INTO TABLE ${env:TMP_db}.asp_${env:project_title}_metric_definitions_stage
;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

SELECT "

\n\n*****\n\nNow preparing table for loading the granular defintions\n\n*****\n\n

"
;

DROP TABLE IF EXISTS ${env:TMP_db}.asp_${env:project_title}_metric_granular_definitions_stage
;

CREATE TABLE ${env:TMP_db}.asp_${env:project_title}_metric_granular_definitions_stage
  (
    run           STRING,
    hive_metric   STRING,
    application   STRING,
    version_note  STRING,
    note          STRING,
    start_date    STRING,
    end_date      STRING,
    condition_1   STRING,
    condition_2   STRING,
    condition_3   STRING,
    condition_4   STRING,
    condition_5   STRING,
    condition_6   STRING
  )
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile TBLPROPERTIES('serialization.null.format'='',"skip.header.line.count"="1")
;
--skips first line of .csv file containing headers

SELECT "

\n\n*****\n\nNow loading in ../bin/${env:project_title}/bin/granular_metrics.tsv\n\n*****\n\n

";

LOAD DATA LOCAL INPATH '../bin/${env:project_title}/bin/granular_metrics.tsv' INTO TABLE ${env:TMP_db}.asp_${env:project_title}_metric_granular_definitions_stage
;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

SELECT "

\n\n*****\n\nNow creating historical definitions and updating current definitions\n\n*****\n\n

"
;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

SELECT"

\n\n*****\n\nSTEP 1: Definitions NULL Placeholders\n\n*****\n\n

"
;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_${env:project_title}_metric_raw
AS
  SELECT
  run,
  hive_metric,
  application,
  version_note,
  note,
  start_date,
  end_date,
  condition_1,
  condition_2,
  condition_3,
  condition_4,
  condition_5,
  condition_6,
  CAST(NULL AS INT) AS version
  FROM ${env:TMP_db}.asp_${env:project_title}_metric_definitions_stage
  WHERE hive_metric <> 'aggregate_placeholder'
    UNION ALL
      SELECT
      run,
      hive_metric,
      application,
      version_note,
      note,
      start_date,
      end_date,
      condition_1,
      condition_2,
      condition_3,
      condition_4,
      condition_5,
      condition_6,
      version
      FROM asp_${env:project_title}_metric_definitions_history
;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_${env:project_title}_metric_nulls
AS
SELECT DISTINCT
CAST(NULL AS STRING) AS run,
np1.hive_metric,
np2.application,
CAST(NULL AS STRING) AS version_note,
CAST(NULL AS STRING) AS note,
CAST(NULL AS STRING) AS start_date,
CAST(NULL AS STRING) AS end_date,
CAST(NULL AS STRING) AS condition_1,
CAST(NULL AS STRING) AS condition_2,
CAST(NULL AS STRING) AS condition_3,
CAST(NULL AS STRING) AS condition_4,
CAST(NULL AS STRING) AS condition_5,
CAST(NULL AS STRING) AS condition_6,
CAST(NULL AS INT) AS version
FROM ${env:TMP_db}.asp_${env:project_title}_metric_raw np1
  JOIN (
    SELECT DISTINCT
    application
    FROM ${env:TMP_db}.asp_${env:project_title}_metric_raw
        ) np2
;

SELECT"

\n\n*****\n\nSTEP 2: Definitions Isolate New Metrics\n\n*****\n\n

"
;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_${env:project_title}_metric_new
AS
SELECT
CAST(nw.run AS STRING) AS run,
CAST(np.hive_metric AS STRING) AS hive_metric,
CAST(np.application AS STRING) AS application,
CAST(nw.version_note AS STRING) AS version_note,
CAST(nw.note AS STRING) AS note,
CAST(nw.start_date AS STRING) AS start_date,
CAST(nw.end_date AS STRING) AS end_date,
CAST(nw.condition_1 AS STRING) AS condition_1,
CAST(nw.condition_2 AS STRING) AS condition_2,
CAST(nw.condition_3 AS STRING) AS condition_3,
CAST(nw.condition_4 AS STRING) AS condition_4,
CAST(nw.condition_5 AS STRING) AS condition_5,
CAST(nw.condition_6 AS STRING) AS condition_6,
CAST(NULL AS INT) AS version
FROM ${env:TMP_db}.asp_${env:project_title}_metric_nulls np
LEFT JOIN ${env:TMP_db}.asp_${env:project_title}_metric_granular_definitions_stage nw
  ON np.application  = nw.application
  AND np.hive_metric = nw.hive_metric
;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_${env:project_title}_metric_old
AS
SELECT
CAST(od.run AS STRING) AS run,
CAST(np.hive_metric AS STRING) AS hive_metric,
CAST(np.application AS STRING) AS application,
CAST(od.version_note AS STRING) AS version_note,
CAST(od.note AS STRING) AS note,
CAST(od.start_date AS STRING) AS start_date,
CAST(od.end_date AS STRING) AS end_date,
CAST(od.condition_1 AS STRING) AS condition_1,
CAST(od.condition_2 AS STRING) AS condition_2,
CAST(od.condition_3 AS STRING) AS condition_3,
CAST(od.condition_4 AS STRING) AS condition_4,
CAST(od.condition_5 AS STRING) AS condition_5,
CAST(od.condition_6 AS STRING) AS condition_6,
CAST(od.version AS INT) AS version
FROM ${env:TMP_db}.asp_${env:project_title}_metric_nulls np
LEFT JOIN asp_${env:project_title}_metric_definitions_history od
  ON np.application  = od.application
  AND np.hive_metric = od.hive_metric
;

SELECT"

\n\n*****\n\nSTEP 3-1: Definitions Update Combined Metrics\n\n*****\n\n

"
;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_${env:project_title}_metrics_combined
AS
select
mn.run AS run,
mn.version_note AS version_note,
mn.note AS note,
mn.start_date AS start_date,
mn.end_date AS end_date,
mn.condition_1 AS condition_1,
mn.condition_2 AS condition_2,
mn.condition_3 AS condition_3,
mn.condition_4 AS condition_4,
mn.condition_5 AS condition_5,
mn.condition_6 AS condition_6,
mn.hive_metric AS hive_metric,
mn.application AS application,
UNIX_TIMESTAMP() AS version
FROM ${env:TMP_db}.asp_${env:project_title}_metric_new mn
LEFT JOIN ${env:TMP_db}.asp_${env:project_title}_metric_old mo
  ON mn.application = mo.application
  AND mn.hive_metric = mo.hive_metric
WHERE (NVL(mn.run,0) <> NVL(mo.run,0)
    OR NVL(mn.condition_1,0) <> NVL(mo.condition_1,0)
    OR NVL(mn.condition_2,0) <> NVL(mo.condition_2,0)
    OR NVL(mn.condition_3,0) <> NVL(mo.condition_3,0)
    OR NVL(mn.condition_4,0) <> NVL(mo.condition_4,0)
    OR NVL(mn.condition_5,0) <> NVL(mo.condition_5,0)
    OR NVL(mn.condition_6,0) <> NVL(mo.condition_6,0))
;

SELECT"

\n\n*****\n\nSTEP 3-2: Insert Updated Combined Metrics\n\n*****\n\n

"
;

INSERT INTO asp_${env:project_title}_metric_definitions_history
SELECT
run,
version_note,
note,
start_date,
end_date,
condition_1,
condition_2,
condition_3,
condition_4,
condition_5,
condition_6,
hive_metric,
application,
version
FROM
${env:TMP_db}.asp_${env:project_title}_metrics_combined
;

SELECT"

\n\n*****\n\nSTEP 4: Definitions Update Current Metrics View\n\n*****\n\n

"
;

DROP VIEW IF EXISTS asp_${env:project_title}_metric_definitions;
CREATE VIEW IF NOT EXISTS asp_${env:project_title}_metric_definitions
AS
select
run,
version_note,
note,
start_date,
end_date,
condition_1,
condition_2,
condition_3,
condition_4,
condition_5,
condition_6,
hive_metric,
application,
CAST(MAX(version) AS INT) as version
from asp_${env:project_title}_metric_definitions_history
GROUP BY
run,
version_note,
note,
start_date,
end_date,
condition_1,
condition_2,
condition_3,
condition_4,
condition_5,
condition_6,
hive_metric,
application
;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

SELECT "

\n\nNow updating granular metrics history and current\n\n

"
;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

SELECT"

\n\n*****\n\nSTEP 1: Granular Defs NULL Placeholders\n\n*****\n\n

"
;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_${env:project_title}_metric_granular_raw
AS
  SELECT
  run,
  hive_metric,
  application,
  version_note,
  note,
  start_date,
  end_date,
  condition_1,
  condition_2,
  condition_3,
  condition_4,
  condition_5,
  condition_6,
  CAST(NULL AS INT) AS version
  FROM ${env:TMP_db}.asp_${env:project_title}_metric_granular_definitions_stage
  WHERE hive_metric <> 'aggregate_placeholder'
    UNION ALL
      SELECT
      run,
      hive_metric,
      application,
      version_note,
      note,
      start_date,
      end_date,
      condition_1,
      condition_2,
      condition_3,
      condition_4,
      condition_5,
      condition_6,
      version
      FROM asp_${env:project_title}_metric_granular_definitions_history
;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_${env:project_title}_metric_granular_nulls
AS
SELECT DISTINCT
CAST(NULL AS STRING) AS run,
np1.hive_metric,
np2.application,
CAST(NULL AS STRING) AS version_note,
CAST(NULL AS STRING) AS note,
CAST(NULL AS STRING) AS start_date,
CAST(NULL AS STRING) AS end_date,
CAST(NULL AS STRING) AS condition_1,
CAST(NULL AS STRING) AS condition_2,
CAST(NULL AS STRING) AS condition_3,
CAST(NULL AS STRING) AS condition_4,
CAST(NULL AS STRING) AS condition_5,
CAST(NULL AS STRING) AS condition_6,
CAST(NULL AS INT) AS version
FROM ${env:TMP_db}.asp_${env:project_title}_metric_granular_raw np1
  JOIN (
    SELECT DISTINCT
    application
    FROM ${env:TMP_db}.asp_${env:project_title}_metric_granular_raw
        ) np2
;

SELECT"

\n\n*****\n\nSTEP 2: Granular Defs Isolate New Metrics\n\n*****\n\n

"
;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_${env:project_title}_metric_granular_new
AS
SELECT
nw.run,
np.hive_metric,
np.application,
nw.version_note,
nw.note,
nw.start_date,
nw.end_date,
nw.condition_1,
nw.condition_2,
nw.condition_3,
nw.condition_4,
nw.condition_5,
nw.condition_6,
CAST(NULL AS INT) AS version
FROM ${env:TMP_db}.asp_${env:project_title}_metric_granular_nulls np
LEFT JOIN ${env:TMP_db}.asp_${env:project_title}_metric_granular_definitions_stage nw
  ON np.application  = nw.application
  AND np.hive_metric = nw.hive_metric
;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_${env:project_title}_metric_granular_old
AS
SELECT
od.run,
np.hive_metric,
np.application,
od.version_note,
od.note,
od.start_date,
od.end_date,
od.condition_1,
od.condition_2,
od.condition_3,
od.condition_4,
od.condition_5,
od.condition_6,
od.version
FROM ${env:TMP_db}.asp_${env:project_title}_metric_granular_nulls np
LEFT JOIN asp_${env:project_title}_metric_granular_definitions_history od
  ON np.application  = od.application
  AND np.hive_metric = od.hive_metric
;


SELECT"

\n\n*****\n\nSTEP 3-1: Granular Defs Update Combined Metrics\n\n*****\n\n

"
;

CREATE TEMPORARY TABLE ${env:TMP_db}.asp_${env:project_title}_metrics_granular_combined
AS
select
mn.run AS run,
mn.version_note AS version_note,
mn.note AS note,
mn.start_date AS start_date,
mn.end_date AS end_date,
mn.condition_1 AS condition_1,
mn.condition_2 AS condition_2,
mn.condition_3 AS condition_3,
mn.condition_4 AS condition_4,
mn.condition_5 AS condition_5,
mn.condition_6 AS condition_6,
mn.hive_metric AS hive_metric,
mn.application AS application,
UNIX_TIMESTAMP() AS version
FROM ${env:TMP_db}.asp_${env:project_title}_metric_granular_new mn
LEFT JOIN ${env:TMP_db}.asp_${env:project_title}_metric_granular_old mo
  ON mn.application = mo.application
  AND mn.hive_metric = mo.hive_metric
WHERE (NVL(mn.run,0) <> NVL(mo.run,0)
    OR NVL(mn.condition_1,0) <> NVL(mo.condition_1,0)
    OR NVL(mn.condition_2,0) <> NVL(mo.condition_2,0)
    OR NVL(mn.condition_3,0) <> NVL(mo.condition_3,0)
    OR NVL(mn.condition_4,0) <> NVL(mo.condition_4,0)
    OR NVL(mn.condition_5,0) <> NVL(mo.condition_5,0)
    OR NVL(mn.condition_6,0) <> NVL(mo.condition_6,0))
;

SELECT"

\n\n*****\n\nSTEP 3-2: Insert Updated Granular Combined Metrics\n\n*****\n\n

"
;

INSERT INTO asp_${env:project_title}_metric_granular_definitions_history
SELECT
run,
version_note,
note,
start_date,
end_date,
condition_1,
condition_2,
condition_3,
condition_4,
condition_5,
condition_6,
hive_metric,
application,
version
FROM
${env:TMP_db}.asp_${env:project_title}_metrics_granular_combined
;

SELECT"

\n\n*****\n\nSTEP 4: Granular Defs Update Current Metrics View\n\n*****\n\n

"
;

DROP VIEW IF EXISTS asp_${env:project_title}_metric_granular_definitions;
CREATE VIEW IF NOT EXISTS asp_${env:project_title}_metric_granular_definitions
AS
select
run,
version_note,
note,
start_date,
end_date,
condition_1,
condition_2,
condition_3,
condition_4,
condition_5,
condition_6,
hive_metric,
application,
CAST(MAX(version) AS INT) as version
from asp_${env:project_title}_metric_granular_definitions_history
GROUP BY
run,
version_note,
note,
start_date,
end_date,
condition_1,
condition_2,
condition_3,
condition_4,
condition_5,
condition_6,
hive_metric,
application
;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

SELECT "

\n\n*****\n\nAll Metric Definition Updates Complete\n\n*****\n\n

"
;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
