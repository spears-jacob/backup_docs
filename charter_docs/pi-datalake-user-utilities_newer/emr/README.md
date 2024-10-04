# emr utilities
Scripts that live here help standarize and simplify preparing and maintaining EMR jobs.

## emr_utils.sh
#### This script sets up the environment in an EMR and includes the following functions.  Whatever is added to the emr_utils.sh script will be available in job steps to use.
- *std_emr_env* - sets up variables, pulls down artifacts, runs process dates script found in this folder
- *run_file* - sorts out the step_array to run each step (hive, pyspark_sql, or shell), including adding spark and log configuration files as defined when running spark
- *csfp* - compute statistics for partition using spark-sql, which provides similar output to the way hive used to show when loading partitions in a job, which is useful to know if the job wrote expected partitions
- *spark_prep* - prepares the spark environment
- several related athena functions (thank you Anna Zheng 😄)
  - *execute_script_in_athena*
  - *get_athena_query_execution_status*
  - *finish_process_with_failure*
- *drop_hive_view_if_exists*
- *execute_script_in_hive*
- *send_email* (thanks Nandi Xie)

## emr_utils.sh Usage: prepare an execute_hql.sh in the following way [(example)](https://gitlab.spectrumflow.net/awspilot/quantum-dasp-jobs/-/blob/master/scp/scp_portals_daily/scripts/execute_hql.sh#L1-4)
- Start with the sourcing of emr_utils.sh execution of the function inside it called ```std_emr_env```
- CADENCE, LAG_DAYS, and LEAD_DAYS are assigned before calling the std_emr_env script because they are used in the subsequent call to process_dates.sh which is a mature script that handles most date scenarios.
- The last part of the call to std_emr_env needs to reflect the suffix of the database that is being referenced in the job.
```
echo -e "\n### EMR Utils - Load EMR Utils and Standard EMR environment (std_emr_env)"; mkdir -p scripts; hdfs dfs -get ${4}/scripts/*"${0: -11:-3}"*.*  ./scripts; source ./scripts/emr_utils-"${0: -11:-3}".sh; AP="${7:-none}";
export CADENCE=daily; export LAG_DAYS=2;
std_emr_env ${1} ${2} ${3} ${4} ${5} ${6} ${AP} "dasp" # change the last argument to your group database suffix
```
- Next, add and adapt any other needed variables and/or function calls needed later.  JOB_NAME needs to be specified at the minimum. [(example)](https://gitlab.spectrumflow.net/awspilot/quantum-dasp-jobs/-/blob/master/scp/scp_portals_daily/scripts/execute_hql.sh#L6-41)
```
export JOB_NAME="You job name here";
function refresh_tableau() { (source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT) }
```
- Then, provide a list of input variables that will be logged as part of the run.  Most of them will be the same in every job, but the order and inclusion of any job-specific variables is configurable.
```
echo -e "
### Input Variables: See below the input script parameters and how this relates to the values of the environment variables in the current shell.
    Feel free to add any that are declared to review them in the stdout and arrange them in preferred order.\n"
input_array=( RUN_DATE ENVIRONMENT GLOBAL_DB DASP_db TMP_db ARTIFACTS_PATH JOB_STEP JOB_STEP_NAME SCRIPT_VERSION RUN_HOUR ADDITIONAL_PARAMS C_ID CLUSTER S_ID STEP SPARK_CONF_FILENAME LOG_CONF_FILENAME RUN_USING CADENCE TZ START_DATE_TZ END_DATE_TZ START_DATE END_DATE) ;
for iv in "${input_array[@]}" ; do echo "export $iv=${!iv};" | tee -a input_variables; done
```

- Finally, prepare an array that runs each job step and call run_file.
## Array for multiple-step jobs: JOB_STEP_NAME FILENAME ACTION COMMENT POSTRUN ONERROR, all required, separated by any number of spaces. There is a default for the last parameter, ${ON_ERROR}, which can be replaced with any shell command(s) such as an email alert. Running csfp for each partition expected to be written in highly recommended.
## For single step jobs, the job step="${JOB_STEP_NAME}". Use ADDITIONAL_PARAMS of RUN_USING=hive if one-time use of the hive engine is required, i.e.: aws lambda invoke --function-name pi-qtm-dasp-stg-portals-ss-metric-agg-run-job response --cli-binary-format raw-in-base64-out --payload '{"JobInput":{"RunDate":"2023-02-02","AdditionalParams":{"RUN_USING":"hive"}}}'


  - Array for [multiple-step jobs](https://gitlab.spectrumflow.net/awspilot/quantum-dasp-jobs/-/blob/master/scp/scp_portals_daily/scripts/execute_hql.sh#L52-66): JOB_STEP_NAME FILENAME ACTION COMMENT POSTRUN ONERROR, all required, separated by any number of spaces.
    - JOB_STEP_NAME - needs to match what is in the main.tf for step name if mulitple steps, otherwise use "${JOB_STEP_NAME}"
    - FILENAME - filename of file to be run (hql, sql, spark, shell, etc)
    - ACTION - how to run the file, current allowed values are ```pyspark_sql```, ```hive```, and ```shell```.  Any more can be added as desired.
    - COMMENT - what is displayed in the stdout when the step is run
    - POSTRUN - if the step is completed successfully, this is run. Running ```csfp``` for each partition in the POSTRUN expected to be written in highly recommended, as it analyses each partition and returns summary statistics.
    - ONERROR - if the step fails, then the ON-ERROR command(s) is run
  - for [single step jobs](https://gitlab.spectrumflow.net/awspilot/quantum-dasp-jobs/-/blob/master/parameterized/portals-ss-metric-agg/scripts/execute_hql.sh#L18), the job step="${JOB_STEP_NAME}"
  - use ADDITIONAL_PARAMS of RUN_USING=hive if one-time use of the hive engine is required, i.e.: aws lambda invoke --function-name pi-qtm-dasp-stg-portals-ss-metric-agg-run-job response --cli-binary-format raw-in-base64-out --payload '{"JobInput":{"RunDate":"2023-02-02","AdditionalParams":{"RUN_USING":"hive"}}}'
```
step_array=("${JOB_STEP_NAME}" "./hql/portals_metric_agg-${SCRIPT_VERSION}.hql" hive "### Running ${JOB_NAME}  ..................." "csfp ${DASP_db}.quantum_metric_agg_portals denver_date ${START_DATE}") "${ON_ERROR}";

run_file
```

- Complete working examples of use
  - multi step job with many additional environment variables and cross-account access: [scp-portals-daily](https://gitlab.spectrumflow.net/awspilot/quantum-dasp-jobs/-/blob/master/scp/scp_portals_daily/scripts/execute_hql.sh)
  - single step job with single partition added per run: [portals-metric-agg](https://gitlab.spectrumflow.net/awspilot/quantum-dasp-jobs/-/blob/master/parameterized/portals-ss-metric-agg/scripts/execute_hql.sh)
  - single step job with several partitions added per run: [ccpa usage](https://gitlab.spectrumflow.net/awspilot/quantum-dasp-jobs/-/blob/master/portals-events/ccpa-usage/scripts/execute_hql.sh)


