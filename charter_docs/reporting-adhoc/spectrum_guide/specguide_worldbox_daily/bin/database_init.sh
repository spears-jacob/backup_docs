#!/bin/bash

find ./init/*.hql -exec hive -v -f {} \;

if [ $? -eq 0 ]; then
	echo "### Job Compliance World Box Monthly database_init job ran successfully  "
else
	echo "### Job Compliance World Box Monthly database_init job FAILED -- Please investigate and re-run"
	exit 1
fi