#!/bin/bash
echo "Clean up HDFS directory"

hdfs dfs -rm -R /incoming/app_figures_raw/*
hdfs dfs -rm -R /apps/hive/warehouse/$TMP_db.db/app_figures_raw/*
hdfs dfs -rm -R /apps/hive/warehouse/$TMP_db.db/app_figures_sentiment/*
