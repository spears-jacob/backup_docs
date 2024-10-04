USE ${env:ENVIRONMENT};

INSERT OVERWRITE TABLE asp_product_monthly_metrics_archive PARTITION(label_date_denver)
SELECT metric_name,
       application_name,
       metric_order,
       application_order,
       report_metric_name ,
       metric_value,
       run_date,
       current_date as archive_date,
       label_date_denver
  FROM asp_product_monthly_last_6months
 WHERE label_date_denver in (
       select distinct label_date_denver
         FROM asp_product_monthly_last_6months
        order by label_date_denver desc
        limit 1
 );

INSERT OVERWRITE TABLE asp_product_monthly_time_archive PARTITION(label_date_denver)
SELECT metric_name,
       application_name,
       application_order,
       pct_less_than2,
       pct_between2_and4,
       pct_between4_and6,
       pct_larger_than6,
       run_date,
       current_date as archive_date,
       label_date_denver
 FROM asp_product_monthly_last_month;
