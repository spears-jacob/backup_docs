#!/bin/bash
echo -e "\n### EMR Utils - Load EMR Utils and Standard EMR environment (std_emr_env)"; mkdir -p scripts; export commit_hash="${BASH_SOURCE: -11:-3}"; hdfs dfs -get ${4}/scripts/*${commit_hash}*.*  ./scripts; source ./scripts/emr_utils-"${commit_hash}".sh; AP="${7:-none}";
export CADENCE=daily; export LAG_DAYS=2; #cadence, lag, and lead days CHANGE WITH JOB REQUIREMENTS and are assigned before calling the std_emr_env script because they are used in the subsequent call to process_dates.sh
std_emr_env ${1} ${2} ${3} ${4} ${5} ${6} ${AP} "dasp" # change the last argument to your group database suffix

# declare and/or override any environment variables unique to the job here
export JOB_NAME="SCP Portals Daily";
if [ "$ENVIRONMENT" == "stg" ]; then
    export CQES=${PROD_ACC_NO}/prod.core_quantum_events_sspp
    export BAES=${PROD_ACC_NO}/prod_ciwifi.billing_acct_equip_snapshot
    export EQUIP=${PROD_ACC_NO}/prod.atom_snapshot_equipment_20190201
    export BILLAGG=stg_dasp.asp_scp_portals_billing_agg
    export LOGSA=stg_dasp.asp_scp_portals_login_subscriber_agg
    export BSA=${PROD_ACC_NO}/prod_ciwifi.scp_billing_subscriber_agg
    export ACCAGG=stg_dasp.asp_scp_portals_acct_agg
    export SNAP=stg_dasp.asp_scp_portals_snapshot
    export LOGIN=stg_dasp.asp_scp_portals_login
    export PAUSE=stg_dasp.asp_scp_portals_pause_acct
    export QMAP=${PROD_ACC_NO}/prod_dasp.quantum_metric_agg_portals
    export SPAT=${PROD_ACC_NO}/prod_ciwifi.scp_plume_ap_telemetry
    export LKP=${PROD_ACC_NO}/prod_ciwifi_lkp.scp_plume_firmware_lkp
elif [ "$ENVIRONMENT" == "prod" ]; then
#    export RUN_USING=hive            #### for testing stg spark against prod hive results
    export CQES=prod.core_quantum_events_sspp
    export BAES=prod_ciwifi.billing_acct_equip_snapshot
    export EQUIP=prod.atom_snapshot_equipment_20190201
    export BILLAGG=prod_dasp.asp_scp_portals_billing_agg
    export LOGSA=prod_dasp.asp_scp_portals_login_subscriber_agg
    export BSA=${PROD_ACC_NO}/prod_ciwifi.scp_billing_subscriber_agg
    export ACCAGG=prod_dasp.asp_scp_portals_acct_agg
    export SNAP=prod_dasp.asp_scp_portals_snapshot
    export LOGIN=prod_dasp.asp_scp_portals_login
    export PAUSE=prod_dasp.asp_scp_portals_pause_acct
    export QMAP=${PROD_ACC_NO}/prod_dasp.quantum_metric_agg_portals
    export SPAT=${PROD_ACC_NO}/prod_ciwifi.scp_plume_ap_telemetry
    export LKP=${PROD_ACC_NO}/prod_ciwifi_lkp.scp_plume_firmware_lkp
fi

function refresh_tableau() { (source ./scripts/tableau_refresh-${SCRIPT_VERSION}.sh $ENVIRONMENT) }

echo -e "
### Input Variables: See below the input script parameters and how this relates to the values of the environment variables in the current shell.
    Feel free to add any that are declared to review them in the stdout and arrange them in preferred order.\n"
input_array=( RUN_DATE ENVIRONMENT GLOBAL_DB DASP_db TMP_db ARTIFACTS_PATH JOB_STEP JOB_STEP_NAME SCRIPT_VERSION RUN_HOUR ADDITIONAL_PARAMS C_ID CLUSTER S_ID STEP RUN_USING CADENCE CQES BAES EQUIP BILLAGG LOGSA BSA ACCAGG SNAP LOGIN PAUSE QMAP SPAT LKP PROD_ACC_NO STG_ACC_NO SPARK_CONF_FILENAME LOG_CONF_FILENAME KEYS_S3_LOCATION JARS_S3_LOCATION SCALA_LIB) ;
for iv in "${input_array[@]}" ; do echo "export $iv=${!iv};" | tee -a input_variables; done

## Array for multiple-step jobs: JOB_STEP_NAME FILENAME ACTION COMMENT POSTRUN ONERROR, all required, separated by any number of spaces. There is a default for the last parameter, "${ON_ERROR}", which can be replaced with any shell command(s) such as an email alert. Running csfp for each partition expected to be written in highly recommended.
## For single step jobs, the job step="${JOB_STEP_NAME}". Use ADDITIONAL_PARAMS of RUN_USING=hive if one-time use of the hive engine is required, i.e.: aws lambda invoke --function-name pi-qtm-dasp-stg-portals-ss-metric-agg-run-job response --cli-binary-format raw-in-base64-out --payload '{"JobInput":{"RunDate":"2023-02-02","AdditionalParams":{"RUN_USING":"hive"}}}'
step_array=(     \
01_scp_portals_daily_action_extract_from_cqesspp ./hql/01_load_scp_portals_action-${SCRIPT_VERSION}.hql                     pyspark_sql "### Running SCP Portals daily action extract, scp_portals_action  ............."  "csfp ${DASP_db}.asp_scp_portals_action                    data_utc_dt    ${START_DATE}"  "${ON_ERROR}" \
02_scp_account_agg_from_action_extract           ./hql/02_load_scp_portals_acct_agg-${SCRIPT_VERSION}.hql                   pyspark_sql "### Running SCP Portals account aggregate from action extract  ................"  "csfp ${DASP_db}.asp_scp_portals_acct_agg                  data_utc_dt    ${START_DATE}"  "${ON_ERROR}" \
03_scp_actions_agg_30daylag_from_action_extract  ./hql/03_load_scp_portals_actions_agg-${SCRIPT_VERSION}.hql                pyspark_sql "### Running SCP Portals action aggregate with 30 day lag from action extract .."  "csfp ${DASP_db}.asp_scp_portals_actions_agg               data_utc_dt    ${START_DATE}"  "${ON_ERROR}" \
04_load_scp_portals_billing_agg                  ./hql/04_load_scp_portals_billing_agg-${SCRIPT_VERSION}.hql                pyspark_sql "### Running SCP Portals billing aggregate ....................................."  "csfp ${DASP_db}.asp_scp_portals_billing_agg               partition_date ${START_DATE}"  "${ON_ERROR}" \
05_scp_pause_schedule_action_agg                 ./hql/05_load_scp_portals_pause_schedule_action_agg-${SCRIPT_VERSION}.hql  pyspark_sql "### Running SCP Portals pause schedule action aggregate ......................."  "csfp ${DASP_db}.asp_scp_portals_pause_schedule_action_agg data_utc_dt    ${START_DATE}"  "${ON_ERROR}" \
06_scp_snapshot                                  ./hql/06_load_scp_portals_snapshot-${SCRIPT_VERSION}.hql                   pyspark_sql "### Running SCP Snapshot aggregate ............................................"  "csfp ${DASP_db}.asp_scp_portals_snapshot                  partition_date ${START_DATE}"  "${ON_ERROR}" \
07_scp_login                                     ./hql/07_load_scp_portals_login-${SCRIPT_VERSION}.hql                      pyspark_sql "### Running SCP login aggregate ..............................................."  "csfp ${DASP_db}.asp_scp_portals_login                     partition_date ${START_DATE}"  "${ON_ERROR}" \
08_scp_login_subscriber_agg                      ./hql/08_load_scp_portals_login_subscriber_agg-${SCRIPT_VERSION}.hql       pyspark_sql "### Running SCP Portals Login subscriber aggregate ............................"  "csfp ${DASP_db}.asp_scp_portals_login_subscriber_agg      partition_date ${START_DATE}"  "${ON_ERROR}" \
09_scp_subscriber_agg                            ./hql/09_load_scp_portals_subscriber_agg-${SCRIPT_VERSION}.hql             pyspark_sql "### Running SCP subscriber aggregate .........................................."  "csfp ${DASP_db}.asp_scp_portals_subscriber_agg            partition_date ${START_DATE}"  "${ON_ERROR}" \
10_scp_portals_visitation_rates                  ./hql/10_load_scp_portals_visitation_rates-${SCRIPT_VERSION}.hql           pyspark_sql "### Running SCP Portals visitation rates ......................................"  "csfp ${DASP_db}.asp_scp_portals_visitation_rates          data_den_dt    ${START_DATE}"  "${ON_ERROR}" \
11_scp_pause                                     ./hql/11_load_scp_portals_pause_acct-${SCRIPT_VERSION}.hql                 pyspark_sql "### Running SCP Pause Account  ................................................"  "csfp ${DASP_db}.asp_scp_portals_pause_acct                data_utc_dt    ${START_DATE}"  "${ON_ERROR}" \
12_scp_pause_agg                                 ./hql/12_load_scp_portals_pause_acct_agg-${SCRIPT_VERSION}.hql             pyspark_sql "### Running SCP Pause Account aggregate ......................................."  "csfp ${DASP_db}.asp_scp_portals_pause_acct_agg            data_utc_dt    ${START_DATE}"  "${ON_ERROR}" \
13_tableau_refresh                                refresh_tableau                                                           shell       "### Refreshing tableau  ......................................................."  "echo 'All Done Refreshing Tableau'"                                                      "${ON_ERROR}" \
);

# defined in the emr-utils, below takes the step array for this step and runs it
run_file