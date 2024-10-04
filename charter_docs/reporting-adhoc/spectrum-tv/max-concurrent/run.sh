#!bin/bash
scp all.csv gv15:all.csv
hdfs dfs -mkdir all
hdfs dfs -rm all/*
hdfs dfs -put all.csv all
