#!/bin/bash
hive --hiveconf start_date='2018-08-01' --hiveconf end_date='2018-09-01' -f msa_state_month.hql >> breakout_by_month.tsv
hive --hiveconf start_date='2018-09-01' --hiveconf end_date='2018-10-01' -f msa_state_month.hql >> breakout_by_month.tsv
hive --hiveconf start_date='2018-10-01' --hiveconf end_date='2018-11-01' -f msa_state_month.hql >> breakout_by_month.tsv
hive --hiveconf start_date='2018-11-01' --hiveconf end_date='2018-12-01' -f msa_state_month.hql >> breakout_by_month.tsv
hive --hiveconf start_date='2018-12-01' --hiveconf end_date='2019-01-01' -f msa_state_month.hql >> breakout_by_month.tsv
hive --hiveconf start_date='2019-01-01' --hiveconf end_date='2019-02-01' -f msa_state_month.hql >> breakout_by_month.tsv
hive --hiveconf start_date='2019-02-01' --hiveconf end_date='2019-03-01' -f msa_state_month.hql >> breakout_by_month.tsv
hive --hiveconf start_date='2019-03-01' --hiveconf end_date='2019-04-01' -f msa_state_month.hql >> breakout_by_month.tsv
hive --hiveconf start_date='2019-04-01' --hiveconf end_date='2019-05-01' -f msa_state_month.hql >> breakout_by_month.tsv
