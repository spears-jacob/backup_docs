#!/bin/bash

hive -f "ctfd_new.hql" >> new.tsv
hive -f "ctfd_old.hql" >> old.tsv
