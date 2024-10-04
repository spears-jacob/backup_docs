#!/bin/bash

hive -v -e "create VIEW IF NOT EXISTS ${DASP_db}.asp_v_daily_report_data AS SELECT * FROM ${DASP_db}.asp_daily_report_data;"

hive -v -e "create VIEW IF NOT EXISTS ${DASP_db}.asp_v_daily_report_data_summary AS SELECT * FROM ${DASP_db}.asp_daily_report_data_summary;"