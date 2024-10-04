CREATE EXTERNAL TABLE IF NOT EXISTS ${db_name}.cs_prod_monthly_fiscal_month_metrics
(
    metric        string COMMENT "whether this row is for call-in rate (cir) or digital first-contact rate (dfcr)",
    visit_type    string COMMENT "",
    value         float  COMMENT "the measured value of the given metric for the given time period and customer type"
)
    PARTITIONED BY (fiscal_month string COMMENT "fiscal month of the metric")
    STORED AS PARQUET
    LOCATION '${s3_location}'
    TBLPROPERTIES ('parquet.compression'='SNAPPY')
