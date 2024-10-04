#!/bin/bash

if [ $# -lt 3 ] || [ $# -gt 3 ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ]; then
    echo "

    Usage: rosetta_build.sh <domain> <project_title> <repo>

    The shell script will conclude if these requirements are not met.
    The domain, project_title, and repo must match values found in rosetta.properties.

      "
    exit 1
fi

# assigns input variables
domain=${1}
project_title=${2}
repo=${3}

grep --quiet domain= rosetta.properties
isDomainPresent=$?
grep --quiet project_title= rosetta.properties
isProject_TitlePresent=$?
grep --quiet repo= rosetta.properties
isRepoPresent=$?


if [ ! -f rosetta.properties ] ; then
    echo "

    rosetta.properties was not found.

    Please prepare one with comma delimited entries for domain, project_title, and repo. "

    exit 1
fi

if [ $isDomainPresent -ne 0 ] || [ $isProject_TitlePresent -ne 0 ] || [ $isRepoPresent -ne 0 ]; then
    echo "

    rosetta.properties was not found to have all the requisite entries.

    Please prepare it with comma delimited entries for domain, project_title, and repo. "

    exit 1
fi


# parses out lists of domains and project titles
export domain_cdl=$(perl -nle'print $& while m{(?<=domain=).*}g' rosetta.properties);
export project_title_cdl=$(perl -nle'print $& while m{(?<=project_title=).*}g' rosetta.properties);
export repo_cdl=$(perl -nle'print $& while m{(?<=repo=).*}g' rosetta.properties);

# puts lists into arrays
IFS=', ' read -r -a domain_array <<< "$domain_cdl"
IFS=', ' read -r -a project_title_array <<< "$project_title_cdl"
IFS=', ' read -r -a repo_array <<< "$repo_cdl"

# tests that domain input matches
if ( ! printf '%s\n' ${domain_array[@]} | grep -q "^$domain$" ) ; then
  echo "
  The domain entry in rosetta.properties did not match what was entered ($domain).

  Please use a domain in the following list or revise rosetta.properties.

  ${domain_array[@]}

  "
  exit 1
fi

# tests that project title input matches
if ( ! printf '%s\n' ${project_title_array[@]} | grep -q "^$project_title$" ) ; then
  echo "
  The project_title entry in rosetta.properties did not match what was entered ($project_title).

  Please use a project_title in the following list or revise rosetta.properties.

  ${project_title_array[@]}

  "
  exit 1
fi

# tests that repo input matches
if ( ! printf '%s\n' ${repo_array[@]} | grep -q "^$repo$" ) ; then
  echo "
  The repo entry in rosetta.properties did not match what was entered ($repo).

  Please use a repo in the following list or revise rosetta.properties.

  ${repo_array[@]}

  "
  exit 1
fi

echo "
#####--   PRE-PYTHON PROCESSING FILE PREPARATION IN SHELL   --#####
"

#Move files to correct directories based on project_title parameter

echo "
#####--   Build parameterized active project file directories using domain: '$domain', project: '$project_title', and repo: $repo   --#####
"

mv ../../../$repo/rosetta_factory/rosetta_engine/bin/$project_title/bin/granular_metrics.tsv bin/granular_metrics.tsv
mv ../../../$repo/rosetta_factory/rosetta_engine/bin/$project_title/bin/qb_input_parameters.csv bin/qb_input_parameters.csv
mv ../../../$repo/rosetta_factory/rosetta_engine/bin/$project_title/hql_templates/* bin/hql_templates
mv ../../../$repo/rosetta_factory/rosetta_engine/bin/$project_title/metric_repo/* bin/metric_repo

# query builder input parameters specifices runs for the query builder
qbipf=bin/qb_input_parameters.csv
# Alias for domain_mappings lookup these are joined to the sourceNames in the qbipf
dmf=bin/domain_mapping.csv

#Sort and strip definitions.tsv to prepare for processing
echo "
#####--   Strip double-quotes from granular_metrics.tsv   --#####
"
sed -i '' 's/\"//g' bin/granular_metrics.tsv

echo "
#####--   Strip trailing tabs from granular_metrics.tsv   --#####
"
sed -i '' -e 's/[[:space:]]*$//' bin/granular_metrics.tsv

echo "
#####--   Sort to correct sort-order for granular_metrics.tsv   --#####
"
(head -n 1 bin/granular_metrics.tsv && tail -n +2 bin/granular_metrics.tsv  | sort -k2,2 -k3,3) > bin/granular_metrics_sorted.tsv
rm -f 'bin/granular_metrics.tsv'
mv bin/granular_metrics_sorted.tsv bin/granular_metrics.tsv

echo "
#####--   Group granular_metrics.tsv on hive_metric   --#####
"
(awk '{ print $2 }' bin/granular_metrics.tsv) > bin/metric_repo/definitions_grouped.tsv

echo "
#####--   Create unique list of hive_metric   --#####
"
echo -e "hive_metric" > bin/metric_repo/metric_unique_list.tsv && (sed 1d bin/metric_repo/definitions_grouped.tsv | uniq) >> bin/metric_repo/metric_unique_list.tsv

echo "
#####--   Create list of only single-row hive_metric   --#####
"
echo -e "hive_metric" > bin/metric_repo/metrics_singles_list.tsv && (sed 1d bin/metric_repo/definitions_grouped.tsv | uniq -u) >> bin/metric_repo/metrics_singles_list.tsv

echo "
#####--   Create list of multi-row hive_metric   --#####
"
echo -e "hive_metric" > bin/metric_repo/metrics_squash_list.tsv && (sed 1d bin/metric_repo/definitions_grouped.tsv | uniq -d) >> bin/metric_repo/metrics_squash_list.tsv

echo "
#####--   Create unique list of multi-row hive_metric   --#####
"
echo -e "hive_metric" > bin/metric_repo/metrics_squash_list_unique.tsv && (sed 1d bin/metric_repo/metrics_squash_list.tsv | uniq) >> bin/metric_repo/metrics_squash_list_unique.tsv

echo "
#####--   RUN PYTHON SCRIPT   --#####
"
python3 bin/build_combined_definitions.py $qbipf $dmf

echo "
#####--   POST-PYTHON PROCESSING FILE CONSOLIDATION IN SHELL   --#####
"

echo "
#####--   Combine HQL Files   --#####
"

group_init=init
group_00=metric_agg
group_01=set_agg_accounts
group_02=set_agg_devices
group_03=set_agg_instances
group_04=set_agg_visits

# Iterate over groups 1-4 for trimming trailing commas
group_array=( $group_01 $group_02 $group_03 $group_04 )
for g in "${group_array[@]}"
do
  echo "
  #####--   Trim trailing commas from $domain $g files   --#####
  "
  sed '$ s/.$//' '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_01.hql'  > '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_01_trimmed.hql'
  rm '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_01.hql'; mv '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_01_trimmed.hql' '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_01.hql'
  sed '$ s/.$//' '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_02.hql'  > '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_02_trimmed.hql'
  rm '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_02.hql'; mv '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_02_trimmed.hql' '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_02.hql'
  sed '$ s/.$//' '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_03.hql'  > '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_03_trimmed.hql'
  rm '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_03.hql'; mv '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_03_trimmed.hql' '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g'_03.hql'
done

echo "
#####--   ${group_init}   --#####
"
rm -f '../../../'$repo'/extract/parameterized/'$domain'_'$project_title'_metric_agg/src/init/'$domain'_'$project_title'_create_tables_and_views.hql'
touch '../../../'$repo'/extract/parameterized/'$domain'_'$project_title'_metric_agg/src/init/'$domain'_'$project_title'_create_tables_and_views.hql'
cat '../temp_file_dump/temp_files/'$domain'_'$project_title'_create_tables_and_views'*'.hql' >> '../../../'$repo'/extract/parameterized/'$domain'_'$project_title'_metric_agg/src/init/'$domain'_'$project_title'_create_tables_and_views.hql'

echo "
#####--   ${group_00}   --#####
"
rm -f '../../../'$repo'/extract/parameterized/'$domain'_'$project_title'_metric_agg/src/'$domain'_'$project_title'_'$group_00'.hql'
touch '../../../'$repo'/extract/parameterized/'$domain'_'$project_title'_metric_agg/src/'$domain'_'$project_title'_'$group_00'.hql'
cat '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$group_00''*'.hql' >> '../../../'$repo'/extract/parameterized/'$domain'_'$project_title'_metric_agg/src/'$domain'_'$project_title'_'$group_00'.hql'

# move groups 1-4 to their destination
for g in "${group_array[@]}"
do
  echo "
  #####--   $g   --#####
  "
  rm -f '../../../'$repo'/extract/parameterized/'$domain'_'$project_title'_set_aggs/src/'$domain'_'$project_title'_'$g'.hql'
  touch '../../../'$repo'/extract/parameterized/'$domain'_'$project_title'_set_aggs/src/'$domain'_'$project_title'_'$g'.hql'
  cat '../temp_file_dump/temp_files/'$domain'_'$project_title'_'$g''*'.hql' >> '../../../'$repo'/extract/parameterized/'$domain'_'$project_title'_set_aggs/src/'$domain'_'$project_title'_'$g'.hql'
done


echo "
#####--   Return active project file directories to source folders   --#####
"

mv bin/granular_metrics.tsv ../../../$repo/rosetta_factory/rosetta_engine/bin/$project_title/bin/granular_metrics.tsv
mv bin/qb_input_parameters.csv ../../../$repo/rosetta_factory/rosetta_engine/bin/$project_title/bin/qb_input_parameters.csv
mv bin/hql_templates/* ../../../$repo/rosetta_factory/rosetta_engine/bin/$project_title/hql_templates
mv bin/metric_repo/* ../../../$repo/rosetta_factory/rosetta_engine/bin/$project_title/metric_repo
#rm ../temp_file_dump/temp_files/*

echo "

______               _   _         ______       _ _     _
| ___ \             | | | |        | ___ \     (_) |   | |          _
| |_/ /___  ___  ___| |_| |_ __ _  | |_/ /_   _ _| | __| | ___ _ __(_)
|    // _ \/ __|/ _ \ __| __/ _' | | ___ \ | | | | |/ _' |/ _ \ '__|
| |\ \ (_) \__ \  __/ |_| || (_| | | |_/ / |_| | | | (_| |  __/ |   _
\_| \_\___/|___/\___|\__|\__\__,_| \____/ \__,_|_|_|\__,_|\___|_|  (_)


           _____                               _
          /  ___|                             | |
          \ '--. _   _  ___ ___ ___  ___ ___  | |
           '--. \ | | |/ __/ __/ _ \/ __/ __| | |
          /\__/ / |_| | (_| (_|  __/\__ \__ \ |_|
          \____/ \__,_|\___\___\___||___/___/ (_)


"
