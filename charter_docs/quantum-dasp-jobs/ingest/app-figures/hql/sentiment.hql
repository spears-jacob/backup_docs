set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

SELECT * FROM ${env:TMP_db}.app_figures_reviews;
