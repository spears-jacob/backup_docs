set hive.exec.dynamic.partition.mode=nonstrict;

MSCK REPAIR TABLE ${env:TMP_db}.app_figures_sentiment;
ANALYZE TABLE ${env:TMP_db}.app_figures_sentiment COMPUTE STATISTICS;
