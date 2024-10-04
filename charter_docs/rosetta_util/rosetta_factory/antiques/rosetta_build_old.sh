#!/bin/bash

# query builder input parameters specifices runs for the query builder
qbipf=bin/qb_input_parameters.csv
# Alias for domain_mappings lookup these are joined to the sourceNames in the qbipf
dmf=bin/domain_mapping.csv

#Sort and strip definitions.tsv to prepare for processing
echo "
#####--   Strip double-quotes from definitions.tsv   --#####
"
sed -i '' 's/\"//g' bin/definitions.tsv

echo "
#####--   Strip trailing tabs from definitions.tsv   --#####
"
sed -i '' -e 's/[[:space:]]*$//' bin/definitions.tsv

echo "
#####--   Sort to correct sort-order for definitions.tsv   --#####
"
(head -n 1 bin/definitions.tsv && tail -n +2 bin/definitions.tsv  | sort -k2,2 -k3,3) > bin/definitions_sorted.tsv
rm -f 'bin/definitions.tsv'
mv bin/definitions_sorted.tsv bin/definitions.tsv

# iterate through input query builder input parameters output queries for each
python3 bin/build_queries.py $qbipf $dmf

domain=portals
group_init=init
group_00=metric_agg
group_01=set_agg_accounts
group_02=set_agg_devices
group_03=set_agg_instances
group_04=set_agg_visits

echo "
#####--   ${group_init}   --#####
"

rm -f '../../extract/parameterized/portals_metric_agg/src/init/create_tables_and_views.hql'
touch '../../extract/parameterized/portals_metric_agg/src/init/create_tables_and_views.hql'
cat '../temp_file_dump/temp_files/'$domain'_create_tables_and_views'*'.hql' >> '../../extract/parameterized/portals_metric_agg/src/init/create_tables_and_views.hql'

echo "
#####--   ${group_00}   --#####
"

rm -f '../../extract/parameterized/portals_metric_agg/src/'$domain'_'$group_00'.hql'
touch '../../extract/parameterized/portals_metric_agg/src/'$domain'_'$group_00'.hql'
cat '../temp_file_dump/temp_files/'$domain'_'$group_00''*'.hql' >> '../../extract/parameterized/portals_metric_agg/src/'$domain'_'$group_00'.hql'

echo "
#####--   ${group_01}   --#####
"

rm -f '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_01'.hql'
touch '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_01'.hql'
cat '../temp_file_dump/temp_files/'$domain'_'$group_01''*'.hql' >> '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_01'.hql'

echo "
#####--   ${group_02}   --#####
"

rm -f '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_02'.hql'
touch '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_02'.hql'
cat '../temp_file_dump/temp_files/'$domain'_'$group_02''*'.hql' >> '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_02'.hql'

echo "
#####--   ${group_03}   --#####
"

rm -f '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_03'.hql'
touch '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_03'.hql'
cat '../temp_file_dump/temp_files/'$domain'_'$group_03''*'.hql' >> '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_03'.hql'

echo "
#####--   ${group_04}   --#####
"

rm -f '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_04'.hql'
touch '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_04'.hql'
cat '../temp_file_dump/temp_files/'$domain'_'$group_04''*'.hql' >> '../../extract/parameterized/portals_set_aggs/src/'$domain'_'$group_04'.hql'
