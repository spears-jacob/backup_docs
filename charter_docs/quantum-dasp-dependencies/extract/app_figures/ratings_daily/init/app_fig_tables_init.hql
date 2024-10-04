USE ${env:ENVIRONMENT};

CREATE TABLE IF NOT EXISTS asp_app_daily_app_figures(
  metric string,
  value bigint)
PARTITIONED BY (
  company string,
  date_denver string);

CREATE TABLE IF NOT EXISTS asp_app_daily_app_figures_reviews(
  company string,
  platform string,
  stars string,
  review string)
PARTITIONED BY (
  date_denver string);

--------------------------------------------------------------------------------
------------------ *** Create daily rating history table *** -------------------
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asp_app_figures_star_ratings_daily
(
  one_star_to_date DECIMAL(10,2),
  two_star_to_date DECIMAL(10,2),
  thr_star_to_date DECIMAL(10,2),
  fou_star_to_date DECIMAL(10,2),
  fiv_star_to_date DECIMAL(10,2),
  one_star_daily DECIMAL(10,2),
  two_star_daily DECIMAL(10,2),
  thr_star_daily DECIMAL(10,2),
  fou_star_daily DECIMAL(10,2),
  fiv_star_daily DECIMAL(10,2),
  product_name STRING
)
PARTITIONED BY
(
  product_id STRING,
  platform STRING,
  version STRING,
  partition_date_denver STRING
)
;
