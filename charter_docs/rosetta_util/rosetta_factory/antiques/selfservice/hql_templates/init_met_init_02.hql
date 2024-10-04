

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
  portals_unique_acct_key STRING,
  activated_experiments map<string,string>,
  technology_type STRING
)
PARTITIONED BY (denver_date STRING)
TBLPROPERTIES ('retention_policy'='Aggregate - With PII (3 Years)')
;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

DROP VIEW IF EXISTS asp_quantum_metric_agg_v;

CREATE VIEW asp_quantum_metric_agg_v AS
select *
from prod.venona_metric_agg_portals;
