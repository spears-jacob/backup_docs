USE ${env:DASP_db};

set hive.exec.dynamic.partition.mode=nonstrict;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
set hive.support.concurrency=false;

INSERT OVERWRITE TABLE ${env:DASP_db}.app_figures_downloads partition (partition_date_denver)
select DISTINCT app_details.product_id as product_id,
  product_name,
  developer,
  icon,
  vendor_identifier,
  ref_no,
  sku,
  package_name,
  store_id,
  store,
  storefront,
  release_date,
  added_date,
  updated_date,
  version,
  active,
  source_external_account_id,
  source_added_timestamp,
  source_active,
  source_hidden,
  source_type,
  all_downloads,
  all_re_downloads,
  all_updates,
  all_returns,
  all_net_downloads,
  all_promos,
  all_revenue,
  all_returns_amount,
  all_edu_downloads,
  all_gifts,
  all_redemptions,
  daily_downloads,
  daily_re_downloads,
  daily_updates,
  daily_returns,
  daily_net_downloads,
  daily_promos,
  daily_revenue,
  daily_returns_amount,
  daily_edu_downloads,
  daily_gifts,
  daily_redemptions,
  us_all_downloads,
  us_all_re_downloads,
  us_all_updates,
  us_all_returns,
  us_all_net_downloads,
  us_all_promos,
  us_all_revenue,
  us_all_returns_amount,
  us_all_edu_downloads,
  us_all_gifts,
  us_all_redemptions,
  us_daily_downloads,
  us_daily_re_downloads,
  us_daily_updates,
  us_daily_returns,
  us_daily_net_downloads,
  us_daily_promos,
  us_daily_revenue,
  us_daily_returns_amount,
  us_daily_edu_downloads,
  us_daily_gifts,
  us_daily_redemptions,
  rating_region,
  one_star,
  two_star,
  thr_star,
  fou_star,
  fiv_star,
  app_details.partition_date_denver as partition_date_denver
from (
  select distinct
    regexp_replace(product_id, "\\..*$", "") AS product_id,
    product_name,
    developer,
    icon,
    vendor_identifier,
    ref_no,
    sku,
    package_name,
    store_id,
    store,
    storefront,
    release_date,
    added_date,
    updated_date,
    version,
    active,
    source_external_account_id,
    source_added_timestamp,
    source_active,
    source_hidden,
    source_type,
    partition_date_denver
  from ${env:TMP_db}.app_figures_app_details
) app_details left join (
  select distinct
    downloads as all_downloads,
    re_downloads as all_re_downloads,
    updates as all_updates,
    returns as all_returns,
    net_downloads as all_net_downloads,
    promos as all_promos,
    revenue as all_revenue,
    returns_amount as all_returns_amount,
    edu_downloads as all_edu_downloads,
    gifts as all_gifts,
    gift_redemptions as all_redemptions,
    regexp_replace(product_id, "\\..*$", "") AS product_id,
    partition_date_denver as partition_date_denver
  from ${env:TMP_db}.app_figures_all_sales
) all_sales
on (app_details.product_id = all_sales.product_id
  and app_details.partition_date_denver = all_sales.partition_date_denver)
left join (
  select distinct
    downloads as daily_downloads,
    re_downloads as daily_re_downloads,
    updates as daily_updates,
    returns as daily_returns,
    net_downloads as daily_net_downloads,
    promos as daily_promos,
    revenue as daily_revenue,
    returns_amount as daily_returns_amount,
    edu_downloads as daily_edu_downloads,
    gifts as daily_gifts,
    gift_redemptions as daily_redemptions,
    regexp_replace(product_id, "\\..*$", "") AS product_id,
    partition_date_denver as partition_date_denver
  from ${env:TMP_db}.app_figures_daily_sales
) daily_sales
on (app_details.product_id = daily_sales.product_id
  and app_details.partition_date_denver = daily_sales.partition_date_denver)
left join (
  select distinct
    downloads as us_all_downloads,
    re_downloads as us_all_re_downloads,
    updates as us_all_updates,
    returns as us_all_returns,
    net_downloads as us_all_net_downloads,
    promos as us_all_promos,
    revenue as us_all_revenue,
    returns_amount as us_all_returns_amount,
    edu_downloads as us_all_edu_downloads,
    gifts as us_all_gifts,
    gift_redemptions as us_all_redemptions,
    regexp_replace(product_id, "\\..*$", "") AS product_id,
    partition_date_denver as partition_date_denver
  from ${env:TMP_db}.app_figures_us_sales
) us_sales
on (app_details.partition_date_denver = us_sales.partition_date_denver
  and app_details.product_id = us_sales.product_id)
left join (
  select distinct
    downloads as us_daily_downloads,
    re_downloads as us_daily_re_downloads,
    updates as us_daily_updates,
    returns as us_daily_returns,
    net_downloads as us_daily_net_downloads,
    promos as us_daily_promos,
    revenue as us_daily_revenue,
    returns_amount as us_daily_returns_amount,
    edu_downloads as us_daily_edu_downloads,
    gifts as us_daily_gifts,
    gift_redemptions as us_daily_redemptions,
    regexp_replace(product_id, "\\..*$", "") AS product_id,
    partition_date_denver as partition_date_denver
  from ${env:TMP_db}.app_figures_us_daily_sales
) us_daily
on (app_details.product_id = us_daily.product_id
  and app_details.partition_date_denver = us_daily.partition_date_denver)
left join (
  select distinct
    regexp_replace(product_id, "\\..*$", "") AS product_id,
    region as rating_region,
    one_star,
    two_star,
    thr_star,
    fou_star,
    fiv_star,
    substr(partition_date_denver, 0, 10) as partition_date_denver
  from ${env:TMP_db}.app_figures_ratings
) ratings
on (app_details.partition_date_denver = ratings.partition_date_denver
  and app_details.product_id = ratings.product_id);

INSERT OVERWRITE TABLE ${env:DASP_db}.app_figures_ranks partition (partition_date_denver)
select distinct
  regexp_replace(product_id, "\\..*$", "") AS product_id,
  country,
  category_store,
  category_store_id,
  category_device,
  category_device_id,
  category_id,
  category_name,
  category_subtype,
  position,
  delta,
  substr(partition_date_denver, 0, 10) as partition_date_denver
  from ${env:TMP_db}.app_figures_daily_ranks;
