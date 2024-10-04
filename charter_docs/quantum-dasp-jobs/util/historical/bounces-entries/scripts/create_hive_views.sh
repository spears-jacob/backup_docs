#!/bin/bash
# TODO: change enviroment to prod
hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_bounces_entries AS SELECT * FROM ${DASP_db}.asp_bounces_entries;"
