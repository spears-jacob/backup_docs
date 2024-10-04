#!/bin/bash
find ./init/*.hql -exec hive -f {} \;
