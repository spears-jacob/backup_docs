USE ${env:DASP_db};

DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_all_sales PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_daily_sales PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_us_sales PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_us_daily_sales PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_app_details PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_ratings PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_daily_ranks PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_reviews PURGE;
DROP TABLE IF EXISTS ${env:TMP_db}.app_figures_sentiment PURGE;
