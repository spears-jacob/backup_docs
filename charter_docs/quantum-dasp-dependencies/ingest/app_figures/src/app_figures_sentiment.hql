set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;

INSERT OVERWRITE TABLE ${env:ENVIRONMENT}.app_figures_sentiment partition (partition_date_denver)
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
  sentiment,
  substr(partition_date_denver, 0, 10)
from ${env:TMP_db}.app_figures_sentiment;
