#!/bin/bash

hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_billing_detail;"