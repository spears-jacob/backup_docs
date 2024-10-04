#!/bin/bash
echo "###DROP asp_v_billing_detail VIEW..."

hive -v -e "DROP VIEW IF EXISTS ${DASP_db}.asp_v_billing_detail;"