#!/bin/bash

. ./scripts/util-${SCRIPT_VERSION}.sh

# TODO Need to remove after first job's execution in specific environment (dev, prod, stg)
drop_hive_view_if_exists "${DASP_db}.vat_cs_quality_check"

execute_script_in_athena "CREATE OR REPLACE VIEW ${DASP_db}.vat_cs_quality_check as
select *
FROM ${DASP_db}.cs_qc

;"
