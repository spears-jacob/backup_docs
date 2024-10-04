USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_counts_definitions
(
  run          STRING,
  comments     STRING,
  hive_name    STRING,
  metric_name  STRING,
  sourceName   STRING,
  label4       STRING,
  label5       STRING,
  condition1   STRING,
  condition2   STRING,
  condition3   STRING,
  condition4   STRING,
  condition5   STRING,
  condition6   STRING,
  run_time     STRING
)
PARTITIONED BY
(
  partition_date_den STRING
)
;

DROP VIEW IF EXISTS asp_v_counts_definitions;

CREATE VIEW IF NOT EXISTS asp_v_counts_definitions AS
SELECT * from prod.asp_counts_definitions;
