CREATE TABLE IF NOT EXISTS ${env:ENVIRONMENT}.cs_prod_monthly_fiscal_month_metrics
(
metric          string  COMMENT "whether this row is for call-in rate (cir) or digital first-contact rate (dfcr)",
customer_type   string  COMMENT "dfcr is split into residential and commercial. cir is combined for all customer types",
value           float   COMMENT "the measured value of the given metric for the given time period and customer type"
)
partitioned by
(
fiscal_month    string  COMMENT "fiscal month of the metric"
)
TBLPROPERTIES (
  'retention_policy'='Aggregate - Without PII (3 Years)'
)
;
