#!/bin/bash
append_path=$1/$2

hdfs dfs -copyToLocal /apps/hive/warehouse/$TMP_db.db/app_figures_sentiment $append_path

echo "list $append_path"
ls -ltr $append_path/app_figures_sentiment

python_env=/data/workspace/yxu/python3
export PATH="/usr/local/bin/anaconda3/bin:$PATH"

# python3 -m conda create --prefix $python_env --clone='/usr/local/bin/anaconda3'
source activate $python_env
# conda install tqdm tensorflow -y
echo "Set python environment to $python_env; Set data source to $append_path"

export LD_LIBRARY_PATH=/usr/local/cuda-8.0/lib64:${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}
# export PATH="/usr/local/bin/anaconda3/bin:$PATH"

# python_env=/usr/local/bin/anaconda3
/opt/glib-2.17/lib/ld-linux-x86-64.so.2 --library-path /opt/glib-2.17/lib:/usr/local/cuda-8.0/lib64:/usr/lib/gcc/x86_64-redhat-linux/4.4.4:/usr/lib64/ $python_env/bin/python $append_path/src/sentiment/sentiment.py $append_path/app_figures_sentiment

ls -ltr $append_path

echo "Copy parsed files to hdfs"
hdfs dfs -rm -R /apps/hive/warehouse/$TMP_db.db/app_figures_sentiment/*
hdfs dfs -copyFromLocal $append_path/app_figures_sentiment/output.csv /apps/hive/warehouse/$TMP_db.db/app_figures_sentiment/output.csv
