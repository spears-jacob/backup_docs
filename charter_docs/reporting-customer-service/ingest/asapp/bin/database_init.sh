#!/bin/bash

export DB_TEMP=$1

#hive -f ../init/1_create_nifi_external_asp_asapp_tables.hql
hive -f ../init/2_create_asp_asapp_nopii_tables.hql
hive -f ../init/3_create_asp_asapp_pii_tables.hql
hive -f ../init/4_create_asp_asapp_pii_temp_tables.hql
hive -f ../init/5_create_asp_asapp_views.hql
