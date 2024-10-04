#!/bin/bash
dbprefix=$1;
find ../init/*.hql -exec hive -f {} \;
