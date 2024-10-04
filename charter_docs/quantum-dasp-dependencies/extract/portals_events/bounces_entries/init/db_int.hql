USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_bounces_entries
( page_name STRING,
  entries INT,
  bounces INT)
PARTITIONED BY
( domain STRING,
  date_denver STRING
)
;

CREATE VIEW IF NOT EXISTS asp_v_bounces_entries
as select * from prod.asp_bounces_entries;
