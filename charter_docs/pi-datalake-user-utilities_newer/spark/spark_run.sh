#!/bin/bash

# Usage: source spark_run.sh
# This script sets up the environment in an EMR for running SQL using PySpark

#ensure location of pyspark_sql_wrapper.py
sourced_path=$(echo -e "${BASH_SOURCE}"); sourced_fn=$(basename ${BASH_SOURCE}); python_file="pyspark_sql_wrapper"; shell_script="spark_run"; ctrfsp=${#sourced_fn}; ctfsp=${#sourced_path}; pathlen=$(( ctfsp - ctrfsp )); script_path=${sourced_path:0:${pathlen}};
ismatch_perl_re "${sourced_fn}" "${shell_script}-\w{8}\.sh";

if [[ "$?" -eq 0 ]];then commit_hash=${sourced_fn: -11:-3}; python_file="${python_file}-${commit_hash}.py"; else python_file="${python_file}.py"; fi
if (! [[ -f "${script_path}${python_file}" ]] ) ; then echo -e "\n\nWas expecting python file here:\n\t${script_path}${python_file}\n\n";  kill -INT $$; fi

# produces echos that include a timestamp
function dtecho() {
        echo -e "\n\n`date '+%F %H:%M:%S'` $@"
}

# sources the spark-env / prepares EMR for running any spark, found in emr_utils
source /usr/lib/spark/conf/spark-env.sh;
spark_prep

function add_spark_conf {
 if [[ $# -ne 1 ]]  ; then echo -e "\n\nUsage: add_spark_conf <filename> \n\n"; kill -INT $$; fi
  cat $1 | sudo tee -a /usr/lib/spark/conf/spark-defaults.conf; echo -e "\n\n Spark Conf added\n\n";
}

function add_log_conf {
 if [[ $# -ne 1 ]]  ; then echo -e "\n\nUsage: add_log_conf <filename> \n\n"; kill -INT $$; fi
  cat $1 | sudo tee -a /usr/lib/spark/conf/log4j2.properties; cat $1 | sudo tee -a /usr/lib/spark/conf/log4j.properties; echo -e "\n\n Log Conf added to both log4j and log4j2 properties files\n\n";
}

function prep_sql {
 if [[ $# -ne 1 ]]  ; then echo -e "\n\nUsage: prep_sql <filename or \"query in quotation marks\"> \n\n"; kill -INT $$; fi
 rt=$(date +%s) # runtime in epoch time, seconds since 1970-01-01
 local cwd=`pwd`; cd `mktemp -d`; local twd=`pwd`; cd ${cwd}; # prepare tmp workspace
 run_file="${twd}/spark_run_${rt}.sql"

 #takes query input and makes it a file or just copies the sql file to a tmp location so this function can be called repeatedly with different variable substitutions
 if [[ -f "${1}" ]] ; then
  cat "${1}" > ${run_file}
 elif [[ ${#1} -gt 8 ]]; then
  echo " non-file query here (or the file path needs adjusting 😉):"
  echo "${1}" > ${run_file}
 else echo -e "\n\nUsage: prep_sql \"query in quotation marks\" \n\n"; kill -INT $$;
 fi

  # replaces ALL ${env:var} hive-style environment and hiveconf variable references with the value of the variable in the shell with the same name
  readarray -t env_array < <( compgen -v ); for envar in "${env_array[@]}"; do sed -i "s/\\\${env:$envar}/${!envar}/g" "${run_file}" 2>/dev/null; done
  readarray -t env_array < <( compgen -v ); for envar in "${env_array[@]}"; do sed -i "s/\\\${hiveconf:$envar}/${!envar}/g" "${run_file}" 2>/dev/null; done

  # deals with ALL remaining cross-account references by removing env:, backticks, substituting variables
  perl -pi.bak -e 's/\$\{env:/\$\{/gi' "${run_file}"; sed -i 's/`//g' "${run_file}"; envsubst <"${run_file}" >"${run_file}_out";  cat "${run_file}_out" > "${run_file}";

  # replaces ALL cross-account  ACCOUNT.db.table references with  `ACCOUNT.db`.table
  perl -pi.bak -e 's!(\d{12}+/\w+)\.(\w+)!`\1`.\2!gi'  "${run_file}";

  # replaces ALL '... -- INLINE COMMENTS'
  perl -pi.bak -e 's/--.*$//gi' "${run_file}";

  # replaces ALL hivevars with value
  cat ${run_file} | egrep -i 'SET\s+hivevar:' > ${run_file}_hivevar_set_statements;
  while read -r line; do
    varname=$(echo $line | perl -pe 's|SET\s+hivevar:(.+)=.+$|$1|g');
    value=$(echo $line | perl -pe 's|SET\s+hivevar:.+=(.+);|$1|g');
    perl -pi.bak -e "s/\\$\{hivevar:${varname}\}/${value}/gi" "${run_file}";
  done <  "${run_file}_hivevar_set_statements"

  # replaces ALL 'SET ...' statements
  perl -pi.bak -e 's/^\w*set.+$//gi' "${run_file}";

  # check that distribute by is in in query
  ioct=$(egrep -i 'insert\s+overwrite'  "${run_file}" | wc -l); dbct=$(egrep -i 'distribute\s+by'  "${run_file}" | wc -l);
  if [[ $ioct -ne 0 ]] && [[ $dbct -eq 0 ]]; then dtecho " ######### Check s3 for splintered files in the output location.\n                    ######### Use DISTRIBUTE BY <partition_field> to remedy this issue.\n\n" ; fi
}

function run_sql_in_pyspark {
  prep_sql "${1}"
  dtecho "SQL to be executed using PySpark is located in the following file:\n ${run_file}\n\n"; cat ${run_file};
  spark-submit ${script_path}${python_file} --sql ${run_file} || { echo "PySpark SQL Failure ${JOB_STEP_NAME}"; exit 101; }
  if [[ $? -ne 0 ]] ; then dtecho "spark job failed\n" ; exit 101 ; else dtecho "Pyspark SQL ran successfully\n" ; fi
}
