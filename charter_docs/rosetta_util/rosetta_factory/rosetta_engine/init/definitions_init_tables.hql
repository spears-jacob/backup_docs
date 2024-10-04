USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_${env:project_title}_metric_definitions
  (
    run           STRING,
    version_note  STRING,
    note          STRING,
    start_date    STRING,
    end_date      STRING,
    condition_1   STRING,
    condition_2   STRING,
    condition_3   STRING,
    condition_4   STRING,
    condition_5   STRING,
    condition_6   STRING,
    hive_metric   STRING,
    application   STRING,
    version       INT
  )
;

CREATE TABLE IF NOT EXISTS asp_${env:project_title}_metric_granular_definitions
  (
    run           STRING,
    version_note  STRING,
    note          STRING,
    start_date    STRING,
    end_date      STRING,
    condition_1   STRING,
    condition_2   STRING,
    condition_3   STRING,
    condition_4   STRING,
    condition_5   STRING,
    condition_6   STRING,
    hive_metric   STRING,
    application   STRING,
    version       INT
  )
;

CREATE TABLE IF NOT EXISTS asp_${env:project_title}_metric_definitions_history
  (
    run           STRING,
    version_note  STRING,
    note          STRING,
    start_date    STRING,
    end_date      STRING,
    condition_1   STRING,
    condition_2   STRING,
    condition_3   STRING,
    condition_4   STRING,
    condition_5   STRING,
    condition_6   STRING,
    hive_metric   STRING,
    application   STRING,
    version       INT
  )
;

CREATE TABLE IF NOT EXISTS asp_${env:project_title}_metric_granular_definitions_history
  (
    run           STRING,
    version_note  STRING,
    note          STRING,
    start_date    STRING,
    end_date      STRING,
    condition_1   STRING,
    condition_2   STRING,
    condition_3   STRING,
    condition_4   STRING,
    condition_5   STRING,
    condition_6   STRING,
    hive_metric   STRING,
    application   STRING,
    version       INT
  )
;

DROP VIEW IF EXISTS asp_v_${env:project_title}_metric_definitions;

CREATE VIEW IF NOT EXISTS asp_v_${env:project_title}_metric_definitions AS
SELECT * from --prod.
asp_${env:project_title}_metric_definitions;

DROP VIEW IF EXISTS asp_v_${env:project_title}_metric_granular_definitions;

CREATE VIEW IF NOT EXISTS asp_v_${env:project_title}_metric_granular_definitions AS
SELECT * from --prod.
asp_${env:project_title}_metric_granular_definitions;
