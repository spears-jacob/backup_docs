USE ${env:DASP_db};

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE ${env:DASP_db}.app_figures_sentiment partition (partition_date_denver)
select distinct
  author,
  title,
  review,
  original_title,
  original_review,
  stars,
  iso,
  version,
  product_name,
  regexp_replace(product_id, "\\..*$", "") AS product_id,
  vendor_id,
  store,
  weight,
  id,
  language,
  0 as sentiment,
  partition_date_denver as review_timestamp,
  substr(partition_date_denver, 0, 10)
from ${env:TMP_db}.app_figures_reviews;
