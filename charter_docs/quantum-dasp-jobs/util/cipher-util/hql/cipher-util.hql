USE ${env:dasp_DB};

SET mapreduce.input.fileinputformat.split.maxsize=68709120;
SET mapreduce.input.fileinputformat.split.minsize=68709120;
SET hive.optimize.sort.dynamic.partition = false;
SET hive.exec.dynamic.partition.mode=nonstrict;
