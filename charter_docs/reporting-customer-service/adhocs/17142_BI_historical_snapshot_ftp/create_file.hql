SET mapreduce.input.fileinputformat.split.maxsize=5368709120;
SET mapreduce.input.fileinputformat.split.minsize=5368709120;
set hive.resultset.use.unique.column.names=false;
set hive.cli.print.header=true;

Select 
*
FROM dev.cs_historical_snapshot_logins_18541
;
