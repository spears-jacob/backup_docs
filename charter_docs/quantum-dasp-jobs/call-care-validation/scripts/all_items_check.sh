#!/bin/bash

#set these variables to whatever tables you're comparing
OLDCALLTABLE=stg_dasp.atom_cs_call_care_data_3_prod_copy
NEWCALLTABLE=stg_red.atom_cs_call_care_data_3

OLDVISITSTABLE=stg_dasp.cs_calls_with_prior_visits_prod_copy
NEWVISITSTABLE=stg_dasp.cs_calls_with_prior_visits

OLDCIRTABLE=stg_dasp.cs_call_in_rate
NEWCIRTABLE=stg_dasp.cs_call_in_rate

TESTDATE=2021-11-21

## mc_check_mso_counts

rm all_items_check_results.txt

echo "1_as_ns_check_prior_visit_segment_counts" >> all_items_check_results.txt
hive -f ../hql/1_as_ns_check_prior_visit_segment_counts.hql --hiveconf new_visits_table=$NEWVISITSTABLE --hiveconf new_call_table=$NEWCALLTABLE >> all_items_check_results.txt
echo "***" >> all_items_check_results.txt

echo "***" >> all_items_check_results.txt
echo "2_as_sdd_dispo_data_discrepancies" >> all_items_check_results.txt
hive -f ../hql/2_as_sdd_dispo_data_discrepancies.hql --hiveconf new_call_table=$NEWCALLTABLE --hiveconf old_call_table=$OLDCALLTABLE >> all_items_check_results.txt
echo "***" >> all_items_check_results.txt

echo "***" >> all_items_check_results.txt
echo "1_cir_cd_check" >> all_items_check_results.txt
hive -f ../hql/1_cir_cd_check.hql --hiveconf new_visits_table=$NEWVISITSTABLE --hiveconf new_cir_table=$NEWCIRTABLE --hiveconf new_call_table=$NEWCALLTABLE --hiveconf daspdb=stg_dasp>> all_items_check_results.txt
echo "***" >> all_items_check_results.txt

echo "***" >> all_items_check_results.txt
echo "1_sd_ak_check_account_key_by_segment_id" >> all_items_check_results.txt
hive -f ../hql/1_sd_ak_check_account_key_by_segment_id.hql --hiveconf new_call_table=$NEWCALLTABLE >> all_items_check_results.txt
echo "***" >> all_items_check_results.txt

echo "***" >> all_items_check_results.txt
echo "1_sd_ct_check" >> all_items_check_results.txt
hive -f ../hql/1_sd_ct_check.hql --hiveconf new_call_table=$NEWCALLTABLE >> all_items_check_results.txt
echo "***" >> all_items_check_results.txt



#bash sd_fd_check_field_discrepancies.sh $OLDCALLTABLE $NEWCALLTABLE $TESTDATE
#echo "When this finishes there will be a .txt file comparing the two call tables"
