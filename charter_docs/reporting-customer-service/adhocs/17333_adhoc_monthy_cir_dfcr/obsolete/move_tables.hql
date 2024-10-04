--CREATE TABLE dev_tmp.prod_monthly_copy as
--SELECT * FROM dev.cs_prod_monthly_fiscal_month_metrics
--;

INSERT INTO TABLE dev.cs_prod_monthly_fiscal_month_metrics PARTITION(fiscal_month)
SELECT metric,customer_type, value, fiscal_month 
FROM dev_tmp.prod_monthly_copy
;

SELECT * FROM dev.cs_prod_monthly_fiscal_month_metrics
;
