#!/bin/bash
# TODO: change enviroment to prod
hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_idm_paths_flow AS SELECT * FROM ${DASP_db}.asp_idm_paths_flow;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_idm_paths_time AS SELECT * FROM ${DASP_db}.asp_idm_paths_time;"

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_idm_paths_metrics AS SELECT * FROM ${DASP_db}.asp_idm_paths_metrics;"
