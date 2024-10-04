#!/bin/bash

hive -v -e "CREATE VIEW IF NOT EXISTS ${DASP_db}.asp_v_specmobile_events AS SELECT * from ${DASP_db}.asp_specmobile_events;"