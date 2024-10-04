#!/bin/bash

echo "### Running 01_metric_lookup job"
hive -f src/01_metric_lookup.hql
echo "### Completed the 01_metric_lookup job...."
